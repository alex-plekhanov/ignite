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

import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.client.ClientTransactions;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link ClientTransactions} over TCP protocol.
 */
class TcpClientTransactions implements ClientTransactions {
    /** Transaction label. */
    private String label;

    /** Channel. */
    private final ReliableChannel ch;

    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Serializer/deserializer. */
    private final ClientUtils serDes;

    /** Constructor. */
    TcpClientTransactions(ReliableChannel ch, ClientBinaryMarshaller marsh) {
        this.ch = ch;
        this.marsh = marsh;

        serDes = new ClientUtils(marsh);
    }

    /** {@inheritDoc} */
    @Override public ClientTransaction txStart() throws ClientException {
        return txStart0(null, null, null, null);
    }

    /** {@inheritDoc} */
    @Override public ClientTransaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
        throws ClientException {
        return txStart0(concurrency, isolation, null, null);
    }

    /** {@inheritDoc} */
    @Override public ClientTransaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation, long
        timeout, int txSize) throws ClientException {
        return txStart0(concurrency, isolation, timeout, txSize);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Tx size.
     */
    private ClientTransaction txStart0(TransactionConcurrency concurrency, TransactionIsolation isolation, Long timeout,
        Integer txSize) throws ClientException {
        return ch.service(ClientOperation.TX_START,
            req -> {
                try (BinaryRawWriterEx writer = new BinaryWriterExImpl(marsh.context(), req, null, null)) {
                    writer.writeInt(concurrency == null ? -1 : concurrency.ordinal());
                    writer.writeInt(isolation == null ? -1 : isolation.ordinal());
                    writer.writeLong(timeout == null ? -1L : timeout);
                    writer.writeInt(txSize == null ? 0 : txSize);
                    writer.writeString(label);
                }
            },
            res -> new TcpClientTransaction(ch, res.readInt())
        );
    }

    /** {@inheritDoc} */
    @Override public ClientTransaction tx() throws ClientException {
        // TODO
        return null;
    }

    /** {@inheritDoc} */
    @Override public ClientTransactions withLabel(String lb) throws ClientException {
        TcpClientTransactions txs = new TcpClientTransactions(ch, marsh);

        txs.label = lb;

        return txs;
    }

    /**
     *
     */
    private static class TcpClientTransaction implements ClientTransaction {
        /** Transaction id. */
        private final int txId;

        /** Channel. */
        private final ReliableChannel ch;

        /**
         * @param ch Channel.
         * @param id Transaction ID.
         */
        private TcpClientTransaction(ReliableChannel ch, int id) {
            this.ch = ch;
            txId = id;
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid xid() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long startTime() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public TransactionIsolation isolation() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public TransactionConcurrency concurrency() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public long timeout() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public @Nullable String label() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void commit() throws ClientException {
            ch.service(ClientOperation.TX_COMMIT, this::writeTxId, null);
        }

        /** {@inheritDoc} */
        @Override public void rollback() throws ClientException {
            ch.service(ClientOperation.TX_ROLLBACK, this::writeTxId, null);
        }

        /** {@inheritDoc} */
        @Override public void close() throws ClientException {
            ch.service(ClientOperation.TX_CLOSE, this::writeTxId, null);
        }

        /**
         * @param out Binary output stream.
         */
        private void writeTxId(BinaryOutputStream out) {
            out.writeInt(txId);
        }
    }
}
