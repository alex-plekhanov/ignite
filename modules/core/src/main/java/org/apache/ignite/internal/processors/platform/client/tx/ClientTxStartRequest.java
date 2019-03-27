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

package org.apache.ignite.internal.processors.platform.client.tx;

import java.util.Optional;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Start transaction request.
 */
public class ClientTxStartRequest extends ClientRequest {
    /** Transaction concurrency control. */
    private final TransactionConcurrency concurrency;

    /** Transaction isolation level. */
    private final TransactionIsolation isolation;

    /** Transaction timeout. */
    private final long timeout;

    /** Number of entries participating in transaction (may be approximate). */
    private final int size;

    /** Transaction label. */
    private final String label;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientTxStartRequest(BinaryRawReader reader) {
        super(reader);

        concurrency = TransactionConcurrency.fromOrdinal(reader.readByte());
        isolation = TransactionIsolation.fromOrdinal(reader.readByte());
        timeout = reader.readLong();
        size = reader.readInt();
        label = reader.readString();
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        TransactionConfiguration cfg = CU.transactionConfiguration(null, ctx.kernalContext().config());

        TransactionConcurrency txConcurrency = concurrency != null ? concurrency : cfg.getDefaultTxConcurrency();
        TransactionIsolation txIsolation = isolation != null ? isolation : cfg.getDefaultTxIsolation();
        long txTimeout = timeout >= 0L ? timeout : cfg.getDefaultTxTimeout();
        int txSize = size >= 0 ? size : 0;

        IgniteTransactions txs = ctx.kernalContext().cache().transactions();

        if (!F.isEmpty(label))
            txs = txs.withLabel(label);

        Transaction tx = txs.txStart(txConcurrency, txIsolation, txTimeout, txSize);

        return super.process(ctx);
    }
}
