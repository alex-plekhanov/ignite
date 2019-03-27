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

package org.apache.ignite.client;

import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.jetbrains.annotations.Nullable;

/**
 * Thin client transaction.
 */
public interface ClientTransaction extends AutoCloseable {
    /**
     * Gets unique identifier for this transaction.
     *
     * @return Transaction UID.
     */
    public IgniteUuid xid();

    /**
     * Start time of this transaction.
     *
     * @return Start time of this transaction on this node.
     */
    public long startTime();

    /**
     * Cache transaction isolation level.
     *
     * @return Isolation level.
     */
    public TransactionIsolation isolation();

    /**
     * Cache transaction concurrency mode.
     *
     * @return Concurrency mode.
     */
    public TransactionConcurrency concurrency();

    /**
     * Gets timeout value in milliseconds for this transaction. If transaction times
     * out prior to it's completion, {@link org.apache.ignite.transactions.TransactionTimeoutException} will be thrown.
     *
     * @return Transaction timeout value.
     */
    public long timeout();

    /**
     * Returns transaction's label.
     * <p>
     * Use {@link ClientTransactions#withLabel(java.lang.String)} to assign a label to a newly created transaction.
     *
     * @return Label.
     */
    @Nullable public String label();

    /**
     * Commits this transaction by initiating {@code two-phase-commit} process.
     *
     * @throws IgniteException If commit failed.
     * @throws TransactionTimeoutException If transaction is timed out.
     * @throws TransactionRollbackException If transaction is automatically rolled back.
     * @throws TransactionOptimisticException If transaction concurrency is {@link TransactionConcurrency#OPTIMISTIC}
     * and commit is optimistically failed.
     * @throws TransactionHeuristicException If transaction has entered an unknown state.
     */
    public void commit() throws ClientException;

    /**
     * Rolls back this transaction.
     * Note, that it's allowed to roll back transaction from any thread at any time.
     *
     * @throws IgniteException If rollback failed.
     */
    public void rollback() throws ClientException;

    /**
     * Ends the transaction. Transaction will be rolled back if it has not been committed.
     *
     * @throws IgniteException If transaction could not be gracefully ended.
     */
    @Override public void close() throws ClientException;
}
