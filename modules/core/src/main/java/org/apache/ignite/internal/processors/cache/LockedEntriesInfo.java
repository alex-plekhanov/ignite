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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Helper class to acquire java level locks on unordered set of entries and avoid deadlocks.
 */
public class LockedEntriesInfo {
    /** Deadlock detection timeout in milliseconds. */
    private static final long DEADLOCK_DETECTION_TIMEOUT = 500L;

    /** Head of queue of locked entries info for each thread. */
    private final LockedEntries lockedEntriesHead = new LockedEntries(null);

    /** Tail of queue of locked entries info for each thread. */
    private final AtomicReference<LockedEntries> lockedEntriesTail = new AtomicReference<>(lockedEntriesHead);

    /**
     * Attempt to lock all provided entries avoiding deadlocks.
     *
     * @param entries Entries to lock.
     * @return {@code True} if entries were successfully locked, {@code false} if possible deadlock detected or
     *      some entries are obsolete (lock attempt should be retried in this case).
     */
    public boolean tryLockEntries(GridCacheEntryEx[] entries) {
        boolean wasInterrupted = false;

        LockedEntries lockedEntries = new LockedEntries(entries);

        addToQueue(lockedEntries);

        try {
            for (int i = 0; i < entries.length; i++) {
                GridCacheEntryEx entry = entries[i];

                if (entry == null)
                    continue;

                boolean retry = false;

                while (true) {
                    if (entry.tryLockEntry(DEADLOCK_DETECTION_TIMEOUT))
                        break; // Successfully locked.
                    else {
                        wasInterrupted |= Thread.interrupted(); // Clear thread interruption flag.

                        if (hasLockCollisions(entry, lockedEntries)) {
                            // Possible deadlock detected, unlock all locked entries and retry again.
                            retry = true;

                            break;
                        }
                        // Possible deadlock not detected, just retry lock on current entry.
                    }
                }

                if (!retry && entry.obsolete()) {
                    entry.unlockEntry();

                    retry = true;
                }

                if (retry) {
                    lockedEntries.lockedIdx = -1;

                    // Unlock all previously locked.
                    for (int j = 0; j < i; j++) {
                        if (entries[j] != null)
                            entries[j].unlockEntry();
                    }

                    return false;
                }

                lockedEntries.lockedIdx = i;
            }

            return true;
        }
        finally {
            if (wasInterrupted)
                Thread.currentThread().interrupt();

            // Already acuired all locks or released all locks here, deadlock is not possible by this thread anymore,
            // can safely delete locks information.
            removeFromQueue(lockedEntries);
        }
    }

    /**
     * @param entry Entry.
     * @param curLockedEntries Current locked entries info.
     * @return {@code True} if another thread holds lock for this entry and started to lock entries earlier.
     */
    private boolean hasLockCollisions(GridCacheEntryEx entry, LockedEntries curLockedEntries) {
        for (LockedEntries item = lockedEntriesHead.next; item != null; item = item.next) {
            if (item == curLockedEntries)
                // Reached current thread item, all other threads started to lock after the current thread.
                return false;

            GridCacheEntryEx[] otherThreadLocks = item.entries;

            int otherThreadLockedIdx = item.lockedIdx;

            // Visibility guarantees provided by volatile lockedIdx field.
            for (int i = 0; i <= otherThreadLockedIdx; i++) {
                if (otherThreadLocks[i] == entry)
                    return true;
            }
        }

        return false;
    }

    /**
     * Add locked entries info for current thread to the tail of the queue.
     *
     * @param lockedEntries Locked entries info.
     */
    private void addToQueue(LockedEntries lockedEntries) {
        // TODO
        while (true) {
            LockedEntries prevTail = lockedEntriesTail.get();

            lockedEntries.prev = prevTail;

            if (LockedEntries.NEXT_FIELD_UPDATER.compareAndSet(prevTail, null, lockedEntries)) {
                lockedEntriesTail.set(lockedEntries);

                return;
            }
        }
    }

    /**
     * Remove locked entries info for current thread from the queue.
     *
     * @param lockedEntries Locked entries info.
     */
    private void removeFromQueue(LockedEntries lockedEntries) {
        // TODO
        while (true) {
            LockedEntries prev = lockedEntries.prev;
            LockedEntries next = lockedEntries.next;

            if (LockedEntries.NEXT_FIELD_UPDATER.compareAndSet(prev, lockedEntries, next)) {
                while (next != null) {
                    if (LockedEntries.PREV_FIELD_UPDATER.compareAndSet(next, lockedEntries, prev))
                        return;

                    next = next.next;
                }

                // Update tail.

                return;
            }

        }
    }

    /** Per-thread locked entries info. */
    private static class LockedEntries {
        /** Locked entries "prev" field updater. */
        private static final AtomicReferenceFieldUpdater<LockedEntries, LockedEntries> PREV_FIELD_UPDATER =
                AtomicReferenceFieldUpdater.newUpdater(LockedEntries.class, LockedEntries.class, "prev");

        /** Locked entries "next" field updater. */
        private static final AtomicReferenceFieldUpdater<LockedEntries, LockedEntries> NEXT_FIELD_UPDATER =
                AtomicReferenceFieldUpdater.newUpdater(LockedEntries.class, LockedEntries.class, "next");

        /** Reference to the previous in queue thread locked reference info. */
        private volatile LockedEntries prev;

        /** Reference to the next in queue thread locked reference info. */
        private volatile LockedEntries next;

        /** Entries to lock. */
        private final GridCacheEntryEx[] entries;

        /** Current locked entry index. */
        private volatile int lockedIdx = -1;

        /** */
        private LockedEntries(GridCacheEntryEx[] entries) {
            this.entries = entries;
        }
    }
}
