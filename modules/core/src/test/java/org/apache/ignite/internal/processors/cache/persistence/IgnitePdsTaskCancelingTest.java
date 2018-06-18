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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Test handle of task canceling with PDS enabled.
 */
public class IgnitePdsTaskCancelingTest extends GridCommonAbstractTest {
    /** Slow file IO enabled. */
    private static final AtomicBoolean slowFileIoEnabled = new AtomicBoolean(false);

    /** Node failure occurs. */
    private static final AtomicBoolean failure = new AtomicBoolean(false);

    /** Number of cache updating tasks. */
    private static final int NUM_TASKS = 16;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailureHandler(new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                return true;
            }
        });

        cfg.setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME).setAffinity(
            new RendezvousAffinityFunction(false, NUM_TASKS / 2)
        ));

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        cfg.setDataStorageConfiguration(dbCfg);

        dbCfg.setFileIOFactory(new SlowIOFactory());

        dbCfg.setDefaultDataRegionConfiguration(new DataRegionConfiguration()
            .setMaxSize(100 * 1024 * 1024)
            .setPersistenceEnabled(true));

        return cfg;
    }

    /**
     * Checks that tasks canceling does not lead to node failure.
     */
    public void testFailNodesOnCanceledTask() throws Exception {
        cleanPersistenceDir();

        try {
            Ignite ig0 = startGrids(4);

            ig0.cluster().active(true);

            Collection<IgniteFuture> cancelFutures = new ArrayList<>(NUM_TASKS);

            IgniteCountDownLatch latch = ig0.countDownLatch("latch", NUM_TASKS, false, true);

            for (int i = 0; i < NUM_TASKS; i++) {
                final Integer key = i;

                cancelFutures.add(ig0.compute().affinityRunAsync(DEFAULT_CACHE_NAME, key,
                    new IgniteRunnable() {
                        @IgniteInstanceResource
                        Ignite ig;

                        @Override public void run() {
                            latch.countDown();

                            latch.await();

                            ig.cache(DEFAULT_CACHE_NAME).put(key, new byte[1024]);
                        }
                    }));
            }

            slowFileIoEnabled.set(true);

            latch.await();

            for (IgniteFuture future: cancelFutures)
                future.cancel();

            slowFileIoEnabled.set(false);

            for (int i = 0; i < NUM_TASKS; i++) {
                final Integer key = i;

                ig0.compute().affinityRun(DEFAULT_CACHE_NAME, key,
                    new IgniteRunnable() {
                        @IgniteInstanceResource
                        Ignite ig;

                        @Override public void run() {
                            ig.cache(DEFAULT_CACHE_NAME).put(key, new byte[1024]);
                        }
                    });
            }

            assertFalse(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return failure.get();
                }
            }, 5_000L));
        }
        finally {
            cleanPersistenceDir();

            stopAllGrids();
        }
    }

    /**
     * Decorated FileIOFactory with slow IO operations.
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

                @Override public int write(byte[] buf, int off, int len) throws IOException {
                    parkForAWhile();

                    return super.write(buf, off, len);
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
                    if(slowFileIoEnabled.get() && slow)
                        LockSupport.parkNanos(5_000_000_000L);
                }
            };
        }
    }
}
