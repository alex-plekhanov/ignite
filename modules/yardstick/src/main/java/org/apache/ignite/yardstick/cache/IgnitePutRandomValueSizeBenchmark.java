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

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 */
public class IgnitePutRandomValueSizeBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** Min value size. */
    private static final int MIN_VAL_SIZE = 64;

    /** Max value size. */
    private static final int MAX_VAL_SIZE = 128;

    /** Different values count. */
    private static final int VALS_COUNT = MAX_VAL_SIZE-MIN_VAL_SIZE;

    /** Values. */
    byte[][] vals = new byte[VALS_COUNT][];

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        for (int i = 0; i < VALS_COUNT; i++)
            vals[i] = new byte[MIN_VAL_SIZE + i];
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteCache<Integer, Object> cache = cacheForOperation();

        int key = nextRandom(args.range());
        byte[] val = vals[nextRandom(VALS_COUNT)];

        cache.put(key, val);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
