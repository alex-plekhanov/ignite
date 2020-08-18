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

package org.apache.ignite.internal.client.thin;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.cache.event.EventType;
import org.apache.ignite.client.ClientContinuousQuery;
import org.apache.ignite.client.ClientContinuousQueryEvent;
import org.apache.ignite.client.ClientContinuousQueryHandler;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.client.thin.TcpClientCache.KEEP_BINARY_FLAG_MASK;

/**
 *
 */
public class ClientContinuousQueryHandlerImpl<K, V> implements ClientContinuousQueryHandler, NotificationListener {
    /** */
    private final ClientContinuousQuery<K, V> qry;

    /** */
    private final ReliableChannel ch;

    /** */
    private final ClientChannel clientCh;

    /** */
    private final boolean keepBinary;

    /** */
    private final int cacheId;

    /** */
    private final ClientUtils utils;

    /** */
    private final Consumer<ClientChannel> chCloseLsnr = this::onChannelClosed;

    /** */
    private final long rsrcId;

    /** */
    ClientContinuousQueryHandlerImpl(
            int cacheId,
            ReliableChannel ch,
            ClientBinaryMarshaller marsh,
            ClientContinuousQuery<K, V> qry,
            boolean keepBinary
    ) {
        this.cacheId = cacheId;
        this.qry = qry;
        this.ch = ch;
        this.keepBinary = keepBinary;
        utils = new ClientUtils(marsh);

        Consumer<PayloadOutputChannel> qryWriter = payloadCh -> {
            BinaryOutputStream out = payloadCh.out();

            out.writeInt(cacheId);
            out.writeByte(keepBinary ? KEEP_BINARY_FLAG_MASK : 0);
            out.writeInt(qry.getPageSize());
            out.writeLong(qry.getTimeInterval());
            out.writeBoolean(qry.isIncludeExpired());

            if (qry.getRemoteFilter() == null)
                out.writeByte(GridBinaryMarshaller.NULL);
            else {
                utils.writeObject(out, qry.getRemoteFilter());
                out.writeByte((byte)1); // Java platform
            }
        };

        ch.addChannelCloseListener(chCloseLsnr);

        try {
            T2<ClientChannel, Long> params = ch.service(
                    ClientOperation.QUERY_CONTINUOUS,
                    qryWriter,
                    res ->  new T2<>(res.clientChannel(), res.in().readLong())
            );

            clientCh = params.get1();
            rsrcId = params.get2();
        }
        catch (ClientError e) {
            ch.removeChannelCloseListener(chCloseLsnr);

            throw new ClientException(e);
        }

        clientCh.addNotificationListener(this);
    }

    /**
     * @param ch Channel.
     */
    private void onChannelClosed(ClientChannel ch) {
        if (ch == clientCh) {
            qry.getLocalListener().onDisconnect();

            U.closeQuiet(this);
        }
    }

    /** {@inheritDoc} */
    @Override public void acceptNotification(
            ClientChannel ch,
            ClientOperation op,
            long rsrcId,
            byte[] payload,
            Exception err
    ) {
        if (op == ClientOperation.QUERY_CONTINUOUS_EVENT_NOTIFICATION && rsrcId == this.rsrcId) {
            if (err == null && payload != null) {
                BinaryInputStream in = new BinaryHeapInputStream(payload);

                int cnt = in.readInt();

                List<ClientContinuousQueryEvent<? extends K, ? extends V>> evts = new ArrayList<>(cnt);

                for (int i = 0; i < cnt; i++) {
                    K key = utils.readObject(in, keepBinary);
                    V oldVal = utils.readObject(in, keepBinary);
                    V val = utils.readObject(in, keepBinary);
                    byte evtTypeByte = in.readByte();

                    EventType evtType;

                    switch (evtTypeByte) {
                        case 0: evtType = EventType.CREATED; break;
                        case 1: evtType = EventType.UPDATED; break;
                        case 2: evtType = EventType.REMOVED; break;
                        case 3: evtType = EventType.EXPIRED; break;
                        default:
                            // We can't throw an exception in notification thread, just skip unknown event types.
                            continue;
                    }

                    evts.add(new ClientContinuousQueryEventImpl<K, V>(key, oldVal, val, evtType));
                }

                // TODO Async notification.
                qry.getLocalListener().onUpdated(evts);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        ch.removeChannelCloseListener(chCloseLsnr);
        clientCh.removeNotificationListener(this);
    }

    /**
     *
     */
    private static class ClientContinuousQueryEventImpl<K, V> implements ClientContinuousQueryEvent<K, V> {
        /** Key. */
        private final K key;

        /** Old value. */
        private final V oldVal;

        /** Value. */
        private final V val;

        /** Event type. */
        private final EventType evtType;

        /**
         *
         */
        private ClientContinuousQueryEventImpl(K key, V oldVal, V val, EventType evtType) {
            this.key = key;
            this.oldVal = oldVal;
            this.val = val;
            this.evtType = evtType;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public V getOldValue() {
            return oldVal;
        }

        /** {@inheritDoc} */
        @Override public EventType getEventType() {
            return evtType;
        }
    }
}
