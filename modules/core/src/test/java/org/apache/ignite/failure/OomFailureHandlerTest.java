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

package org.apache.ignite.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Out of memory error failure handler test.
 */
public class OomFailureHandlerTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new DummyFailureHandler();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test OOME in IgniteCompute.
     */
    public void testComputeOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        try {
            IgniteFuture<Boolean> res = ignite0.compute(ignite0.cluster().forNodeId(ignite1.cluster().localNode().id()))
                .callAsync(new IgniteCallable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        throw new OutOfMemoryError();
                    }
                });

            res.get();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertFalse(dummyFailureHandler(ignite0).failure());
        assertTrue(dummyFailureHandler(ignite1).failure());
    }

    /**
     * Gets dummy failure handler for ignite instance.
     *
     * @param ignite Ignite.
     */
    private static DummyFailureHandler dummyFailureHandler(Ignite ignite) {
        return (DummyFailureHandler)ignite.configuration().getFailureHandler();
    }
    /**
     *
     */
    private static class DummyFailureHandler implements FailureHandler {
        /** Failure. */
        private boolean failure;

        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            failure = true;

            return true;
        }

        /**
         * @return Failure.
         */
        public boolean failure() {
            return failure;
        }
    }
}
