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
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionHeuristicException;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.transactions.TransactionTimeoutException;

/**
 * Thin client transaction.
 */
public interface ClientTransaction extends AutoCloseable {
    /**
     * Commits this transaction.
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
\     *
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
