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
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.internal.client.thin.ProtocolVersion.V1_5_0;

/**
 * Implementation of {@link ClientTransactions} over TCP protocol.
 */
class TcpClientTransactions implements ClientTransactions {
    /** Transaction label. */
    private String lb;

    /** Channel. */
    private final ReliableChannel ch;

    /** Marshaller. */
    private final ClientBinaryMarshaller marsh;

    /** Current thread transaction. */
    private final ThreadLocal<TcpClientTransaction> tx = new ThreadLocal<>();

    /** Constructor. */
    TcpClientTransactions(ReliableChannel ch, ClientBinaryMarshaller marsh) {
        this.ch = ch;
        this.marsh = marsh;
    }

    /** {@inheritDoc} */
    @Override public ClientTransaction txStart() {
        return txStart0(null, null, null);
    }

    /** {@inheritDoc} */
    @Override public ClientTransaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        return txStart0(concurrency, isolation, null);
    }

    /** {@inheritDoc} */
    @Override public ClientTransaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
        long timeout) {
        return txStart0(concurrency, isolation, timeout);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     */
    private ClientTransaction txStart0(TransactionConcurrency concurrency, TransactionIsolation isolation, Long timeout) {
        if (tx() != null)
            throw new ClientException("Transaction is already started by current thread.");

        TcpClientTransaction tx0 = ch.service(ClientOperation.TX_START,
            req -> {
                if (ch.serverVersion().compareTo(V1_5_0) < 0) {
                    throw new ClientProtocolError(String.format("Transactions not supported by server protocol " +
                        "version %s, required version %s", ch.serverVersion(), V1_5_0));
                }

                try (BinaryRawWriterEx writer = new BinaryWriterExImpl(marsh.context(), req, null, null)) {
                    writer.writeByte((byte)(concurrency == null ? -1 : concurrency.ordinal()));
                    writer.writeByte((byte)(isolation == null ? -1 : isolation.ordinal()));
                    writer.writeLong(timeout == null ? -1L : timeout);
                    writer.writeString(lb);
                }
            },
            res -> new TcpClientTransaction(res.readInt(), ch.clientChannel())
        );

        tx.set(tx0);

        return tx0;
    }

    /** {@inheritDoc} */
    @Override public ClientTransactions withLabel(String lb) {
        if (lb == null)
            throw new NullPointerException();

        TcpClientTransactions txs = new TcpClientTransactions(ch, marsh);

        txs.lb = lb;

        return txs;
    }

    /**
     * Current thread transaction.
     */
    TcpClientTransaction tx() {
        TcpClientTransaction tx0 = tx.get();

        // Also check isClosed() flag, since transaction can be closed by another thread.
        return tx0 == null || tx0.isClosed() ? null : tx0;
    }

    /**
     *
     */
    class TcpClientTransaction implements ClientTransaction {
        /** Transaction id. */
        private final int txId;

        /** Client channel. */
        private final ClientChannel clientCh;

        /** Transaction is closed. */
        private volatile boolean closed;

        /**
         * @param id Transaction ID.
         * @param clientCh Client channel.
         */
        private TcpClientTransaction(int id, ClientChannel clientCh) {
            txId = id;
            this.clientCh = clientCh;
        }

        /** {@inheritDoc} */
        @Override public void commit() {
            if (tx.get() == null)
                throw new ClientException("The transaction is already closed");

            if (tx.get() != this)
                throw new ClientException("You can commit transaction only from the thread it was started");

            sendTxStatus(true);
        }

        /** {@inheritDoc} */
        @Override public void rollback() {
            sendTxStatus(false);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            try {
                sendTxStatus(false);
            }
            catch (Exception ignore) {
                // No-op.
            }

            closed = true;

            if (tx.get() == this)
                tx.set(null);
        }

        /**
         * @param committed Committed.
         */
        private void sendTxStatus(boolean committed) {
            ch.service(ClientOperation.TX_END,
                req -> {
                    if (clientCh != ch.clientChannel())
                        throw new ClientException("TODO fail");

                    req.writeInt(txId);
                    req.writeBoolean(committed);
                }, null);
        }

        /**
         * Tx ID.
         */
        int txId() {
            return txId;
        }

        /**
         * Client channel.
         */
        ClientChannel clientChannel() {
            return clientCh;
        }

        /**
         * Is transaction closed.
         */
        boolean isClosed() {
            return closed;
        }
    }
}
