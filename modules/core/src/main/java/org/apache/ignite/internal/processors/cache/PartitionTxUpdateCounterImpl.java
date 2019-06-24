/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Update counter implementation used for transactional cache groups in persistent mode.
 * <p>
 * Implements new partition update counter flow to avoid situations when:
 * <ol>
 *     <li>update counter could be incremented and persisted while corresponding update is not recorded to WAL.</li>
 *     <li>update counter could be prematurely incremented causing missed rebalancing.</li>
 * </ol>
 * All these situations are sources of partitions desync.
 * <p>
 * Below a short description of new flow:
 * <ol>
 *     <li>Update counter is <i>reserved</i> for each update in partition on tx prepare phase (which always happens
 *     on primary partition owner). Reservation causes HWM increment.</li>
 *     <li>Reserved counter values are propagated on backup nodes and stored in backup transactions.</li>
 *     <li>On commit reserved counters are assigned to cache entries.</li>
 *     <li>LWM is incremented ONLY after corresponding WAL data record for each entry was written.</li>
 *     <li>In case of rollback (manual or during tx recovery on node failure) reserved updates are also applied and
 *     logged to WAL using {@link RollbackRecord} for further recovery purposes.</li>
 * </ol>
 */
public class PartitionTxUpdateCounterImpl implements PartitionUpdateCounter {
    /**
     * Max allowed missed updates. Overflow will trigger critical failure handler to prevent OOM.
     */
    public static final int MAX_MISSED_UPDATES = 10_000;

    /** Counter updates serialization version. */
    private static final byte VERSION = 1;

    /** Queue of applied out of order counter updates. */
    private NavigableMap<Long, Long> queue = new TreeMap<>();

    /** Lock. TODO */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** LWM. */
    private final AtomicLong cntr = new AtomicLong();

    /** HWM. */
    private final AtomicLong reserveCntr = new AtomicLong();

    /**
     * Initial counter points to last sequential update after WAL recovery.
     * @deprecated TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11794
     */
    @Deprecated private long initCntr;

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        cntr.set(initUpdCntr);

        initCntr = initUpdCntr;

        queue = fromBytes(cntrUpdData);
    }

    /** {@inheritDoc} */
    @Override public long initial() {
        return initCntr;
    }

    /** {@inheritDoc} */
    @Override public long get() {
        return cntr.get();
    }

    /**
     * @return Next update counter. For tx mode called by {@link DataStreamerImpl} IsolatedUpdater.
     */
    @Override public long next() {
        long next = cntr.incrementAndGet();

        reserveCntr.set(next);

        return next;
    }

    /** {@inheritDoc} */
    @Override public void update(long val) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            Map.Entry<Long, Long> lastUpdate = queue.lastEntry();

            // Absolute counter should be not less than last applied update.
            // Otherwise supplier doesn't contain some updates and rebalancing couldn't restore consistency.
            // Best behavior is to stop node by failure handler in such a case.
            if (lastUpdate != null && val < lastUpdate.getKey() + lastUpdate.getValue())
                throw new IgniteCheckedException("Failed to update the counter [newVal=" + val + ", curState=" + this + ']');

            long cur = cntr.get();

            // Reserved update counter is updated only on exchange or in non-tx mode.
            reserveCntr.set(Math.max(cur, val));

            if (val <= cur)
                return;

            cntr.set(val);

            queue.clear();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean update(long start, long delta) {
        lock.writeLock().lock();

        // TODO make thread safe
        try {
            long cur = cntr.get(), next;

            if (cur > start)
                return false;

            if (cur < start) {
                // Merge with next.
                Long nextDelta = queue.remove(start + delta);

                if (nextDelta != null)
                    delta += nextDelta;

                Map.Entry<Long, Long> last = queue.lowerEntry(start);

                // Merge with previous, possibly modifying previous.
                if (last != null) {
                    if (last.getKey() + last.getValue() == start)
                        queue.replace(last.getKey(), last.getValue(), last.getValue() + delta);
/*
                    else if (last.within(start) && last.within(start + delta - 1))
                        return false;
*/
                }
                else {
                    if (queue.size() >= MAX_MISSED_UPDATES) // Should trigger failure handler.
                        throw new IgniteException("Too many gaps [cntr=" + this + ']');

                    queue.put(start, delta);
                }

                return true;
            }

            next = start + delta;

            while (true) {
                boolean res = cntr.compareAndSet(cur, next);

                assert res;

                Long nextDelta = queue.remove(next);

                if (nextDelta == null)
                    return true;

                cur = next;

                next += nextDelta;
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void updateInitial(long start, long delta) {
        update(start, delta);

        initCntr = get();
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        lock.writeLock().lock();

        try {
            Map.Entry<Long, Long> item = queue.pollFirstEntry();

            GridLongList gaps = null;

            while (item != null) {
                if (gaps == null)
                    gaps = new GridLongList((queue.size() + 1) * 2);

                long start = cntr.get() + 1;
                long end = item.getKey();

                gaps.add(start);
                gaps.add(end);

                // Close pending ranges.
                cntr.set(item.getKey() + item.getValue());

                item = queue.pollFirstEntry();
            }

            return gaps;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        long cntr = get();

        long reserved = reserveCntr.getAndAdd(delta);

        assert reserved >= cntr : "LWM after HWM: lwm=" + cntr + ", hwm=" + reserved;

        return reserved;
    }

    /** {@inheritDoc} */
    @Override public long next(long delta) {
        return cntr.getAndAdd(delta);
    }

    /** {@inheritDoc} */
    @Override public boolean sequential() {
        return queue.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public @Nullable byte[] getBytes() {
        lock.writeLock().lock();

        try {
            if (queue.isEmpty())
                return null;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();

            DataOutputStream dos = new DataOutputStream(bos);

            dos.writeByte(VERSION);

            int size = queue.size();

            dos.writeInt(size);

            for (Map.Entry<Long, Long> item : queue.entrySet()) {
                dos.writeLong(item.getKey());
                dos.writeLong(item.getValue());
            }

            bos.close();

            return bos.toByteArray();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param raw Raw bytes.
     */
    private @Nullable NavigableMap<Long, Long> fromBytes(@Nullable byte[] raw) {
        NavigableMap<Long, Long> ret = new TreeMap<>();

        if (raw == null)
            return ret;

        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(raw);

            DataInputStream dis = new DataInputStream(bis);

            dis.readByte(); // Version.

            int cnt = dis.readInt(); // Holes count.

            while(cnt-- > 0)
                ret.put(dis.readLong(), dis.readLong());

            return ret;
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        lock.writeLock().lock();

        try {
            cntr.set(0);

            reserveCntr.set(0);

            queue = new TreeMap<>();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Update counter task. Update from start value by delta value.
     */
    private static class Item implements Comparable<Item> {
        /** */
        private long start;

        /** */
        private long delta;

        /**
         * @param start Start value.
         * @param delta Delta value.
         */
        private Item(long start, long delta) {
            this.start = start;
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(@NotNull Item o) {
            return Long.compare(this.start, o.start);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Item [" +
                "start=" + start +
                ", delta=" + delta +
                ']';
        }

        /** */
        public long start() {
            return start;
        }

        /** */
        public long delta() {
            return delta;
        }

        /** */
        public long absolute() {
            return start + delta;
        }

        /** */
        public boolean within(long cntr) {
            return cntr - start < delta;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Item item = (Item)o;

            if (start != item.start)
                return false;

            return  (delta != item.delta);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        PartitionTxUpdateCounterImpl cntr = (PartitionTxUpdateCounterImpl)o;

        if (!queue.equals(cntr.queue))
            return false;

        return this.cntr.get() == cntr.cntr.get();
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return reserveCntr.get();
    }

    /** {@inheritDoc} */
    @Override public Iterator<long[]> iterator() {
        return F.iterator(queue.entrySet().iterator(), item -> new long[] {item.getKey(), item.getValue()}, true);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Counter [lwm=" + get() + ", holes=" + queue + ", hwm=" + reserveCntr.get() + ']';
    }
}
