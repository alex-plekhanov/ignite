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

package org.apache.ignite.internal.processors.cache.distributed;

import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.eventstorage.NoopEventStorageSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.events.EventType.EVT_CACHE_ENTRY_CREATED;

/**
 *
 */
public class CacheTxFailure extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                new CacheConfiguration()
                    .setName(DEFAULT_CACHE_NAME)
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            )
            .setEventStorageSpi(
                new NoopEventStorageSpi() {
                    @Override public void record(Event evt) throws IgniteSpiException {
                        if (evt.type() == EVT_CACHE_ENTRY_CREATED && getTestIgniteInstanceIndex(igniteInstanceName) == 1)
                            throw new CacheException();
                    }
                }
            );
    }

    /**
     *
     */
    public void testTxFailure() throws Exception {
        startGrids(2);

        IgniteCache cache0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache cache1 = grid(1).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++) {
            try (Transaction tx = grid(0).transactions().txStart(TransactionConcurrency.PESSIMISTIC, TransactionIsolation.REPEATABLE_READ)) {
                cache0.put(primaryKey(cache1), 0);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        }
}
