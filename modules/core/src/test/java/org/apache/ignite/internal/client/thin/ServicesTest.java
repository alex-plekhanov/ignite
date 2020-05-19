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

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks service invocation for thin client.
 */
public class ServicesTest extends GridCommonAbstractTest {
    /** Client connector address. */
    private static final String CLIENT_CONN_ADDR = "127.0.0.1:" + ClientConnectorConfiguration.DFLT_PORT;

    /** Test service name. */
    private static final String TEST_SERVICE_NAME = "test_svc";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        grid(0).services().deployClusterSingleton(TEST_SERVICE_NAME, new TestService());
    }

    /**
     * Test.
     */
    @Test
    public void testServiceInvocation() throws Exception {
        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(CLIENT_CONN_ADDR))){
            TestServiceInterface svc = client.services().serviceProxy(TEST_SERVICE_NAME, TestServiceInterface.class);

            log.info(">>>> " + svc.testMethod("test"));

            log.info(">>>> " + svc.testMethod(123));

            log.info(">>>> " + svc.testMethod(new Object()));

            log.info(">>>> " + svc.testMethod((Object)null));
        }
    }

    /**
     *
     */
    public static interface TestServiceInterface {
        /**
         * @param val Value.
         */
        public String testMethod(String val);

        /**
         * @param val Value.
         */
        public String testMethod(Object val);

        /**
         * @param val Value.
         */
        public int testMethod(int val);
    }

    /**
     *
     */
    public static class TestService implements Service, TestServiceInterface {
        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public String testMethod(String val) {
            return "String " + val;
        }

        /** {@inheritDoc} */
        @Override public String testMethod(Object val) {
            return "Object " + val;
        }

        /** {@inheritDoc} */
        @Override public int testMethod(int val) {
            return val;
        }
    }
}
