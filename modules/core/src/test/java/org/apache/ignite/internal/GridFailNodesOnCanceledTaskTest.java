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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.*;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 */
public class GridFailNodesOnCanceledTaskTest extends GridCommonAbstractTest {
    /** Slow file io enabled. */
    private static final AtomicBoolean slowFileIoEnabled = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureHandler(new StopNodeFailureHandler());

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        cfg.setDataStorageConfiguration(dbCfg);

        dbCfg.setFileIOFactory(new SlowIOFactory());

        dbCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100 * 1024 * 1024)
            .setPersistenceEnabled(true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        slowFileIoEnabled.set(false);

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        slowFileIoEnabled.set(false);

        cleanPersistenceDir();
    }

    /**
     *
     */
    public void testFailNodesOnCanceledTask() throws Exception {
        try {
            IgniteEx ig0 = (IgniteEx)startGrids(4);

            ig0.cluster().active(true);

            int NUM_TASKS = 10;

            ig0.createCache(createCacheConfiguration("cache-0", NUM_TASKS));

            Collection<IgniteFuture> cancelFutures = new ArrayList<>(NUM_TASKS);

            IgniteCountDownLatch latch = ig0.countDownLatch("create grid latch", NUM_TASKS, false, true);

            for (int i = 0; i < NUM_TASKS; i++) {
                int cnt = i;

                cancelFutures.add(ig0.compute().affinityCallAsync(Collections.singleton("cache-0"), cnt,
                    new IgniteCallable<Object>() {
                        @IgniteInstanceResource
                        Ignite ig;

                        @Override public Object call() throws Exception {
                            latch.countDown();

                            latch.await();

                            ig.cache("cache-0").put(cnt, new byte[1024]);

                            return null;
                        }
                    }));
            }

            slowFileIoEnabled.set(true);

            latch.await();

            for (IgniteFuture future: cancelFutures)
                future.cancel();

            int numOfRetries = 50;

            while(ig0.cluster().nodes().size() == 4 && numOfRetries > 0){
                Thread.sleep(1000);

                numOfRetries--;
            }

            assertTrue("Nodes have left", ig0.cluster().nodes().size() == 4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param name Name.
     * @param partNum Partition number.
     */
    private CacheConfiguration<Integer, Object> createCacheConfiguration(String name, int partNum) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(name);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, partNum));

        return ccfg;
    }

    /**
     *
     */
    private static class SlowIOFactory implements FileIOFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** {@inheritDoc} */
        @Override public FileIO create(File file) throws IOException {
            return create(file, CREATE, READ, WRITE);
        }

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... openOption) throws IOException {
            final FileIO delegate = delegateFactory.create(file, openOption);

            final boolean slow = file.getName().contains(".bin");

            return new FileIODecorator(delegate) {
                @Override public int write(ByteBuffer srcBuf) throws IOException {
                    parkForAWhile();

                    return super.write(srcBuf);
                }

                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    parkForAWhile();

                    return super.write(srcBuf, position);
                }

                @Override public void write(byte[] buf, int off, int len) throws IOException {
                    parkForAWhile();

                    super.write(buf, off, len);
                }

                @Override public int read(ByteBuffer destBuf) throws IOException {
                    parkForAWhile();

                    return super.read(destBuf);
                }

                @Override public int read(ByteBuffer destBuf, long position) throws IOException {
                    parkForAWhile();

                    return super.read(destBuf, position);
                }

                @Override public int read(byte[] buf, int off, int len) throws IOException {
                    parkForAWhile();

                    return super.read(buf, off, len);
                }

                private void parkForAWhile() {
                    if(slowFileIoEnabled.get() && slow) {
                        for (int i = 0; i < 100; i++)
                            LockSupport.parkNanos(50_000_000);
                    }
                }
            };
        }
    }

}
