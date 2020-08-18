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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.event.EventType;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientContinuousQuery;
import org.apache.ignite.client.ClientContinuousQueryEvent;
import org.apache.ignite.client.ClientContinuousQueryListener;
import org.apache.ignite.client.IgniteClient;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class ContinuousQueriesTest extends AbstractThinClientTest {
    /** Timeout. */
    private static long TIMEOUT = 1_000L;

    /** Test continuous queries. */
    @Test
    public void testContinuousQueries() throws Exception {
        Ignite ignite = startGrids(3);
        Map<Integer, Integer> locMap = new ConcurrentHashMap<>();

        try (IgniteClient client = startClient(0, 1, 2)) {
            ClientCache<Integer, Integer> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

            AtomicBoolean disconnected = new AtomicBoolean();

            cache.continuousQuery(new ClientContinuousQuery<Integer, Integer>().setLocalListener(
                    new ClientContinuousQueryListener<Integer, Integer>() {
                        @Override public void onUpdated(Iterable<ClientContinuousQueryEvent<? extends Integer, ? extends Integer>> events) {
                            for (ClientContinuousQueryEvent<? extends Integer, ? extends Integer> evt : events) {
                                if (evt.getEventType() == EventType.CREATED || evt.getEventType() == EventType.UPDATED)
                                    locMap.put(evt.getKey(), evt.getValue());
                                else if (evt.getEventType() == EventType.REMOVED || evt.getEventType() == EventType.EXPIRED)
                                    locMap.remove(evt.getKey());
                            }
                        }

                        @Override public void onDisconnect() {
                            disconnected.set(true);
                        }
                    }
            ));

            assertFalse(locMap.containsKey(0));

            cache.put(0, 0);

            assertTrue(waitForCondition(() -> locMap.containsKey(0) && locMap.get(0) == 0, TIMEOUT));

            cache.put(0, 1);

            assertTrue(waitForCondition(() -> locMap.get(0) == 1, TIMEOUT));

            cache.remove(0);

            assertTrue(waitForCondition(() -> !locMap.containsKey(0), TIMEOUT));
        }
    }
}
