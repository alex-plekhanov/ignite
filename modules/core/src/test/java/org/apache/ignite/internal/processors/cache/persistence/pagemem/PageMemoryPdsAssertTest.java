/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointLockStateChecker;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointWriteProgressSupplier;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.failure.FailureProcessor;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.mockito.Mockito;

/**
 *
 */
public class PageMemoryPdsAssertTest extends GridCommonAbstractTest {
    /** Page size. */
    private static final int PAGE_SIZE = 1024;

    /** Max memory size. */
    private static final int MAX_SIZE = 16 * 1024 * 1024;

    /**
     * @param memory Page memory.
     */
    private FullPageId allocatePage(PageMemoryImpl memory) throws IgniteCheckedException {
        long pageId = memory.allocatePage(1, PageIdAllocator.INDEX_PARTITION, PageIdAllocator.FLAG_IDX);

        FullPageId fullPageId = new FullPageId(pageId, 1);

        acquireAndReleaseWriteLock(memory, fullPageId); //to set page id, otherwise we would fail with assertion error

        return fullPageId;
    }

    /**
     * @throws Exception if failed.
     */
    public void testPdsAssert() throws Exception {
        PageMemoryImpl memory = createPageMemory(PageMemoryImpl.ThrottlingPolicy.DISABLED);

        List<FullPageId> pages = new ArrayList<>();

        // Allocate as much as we can, and store allocated pages.
        memory.beginCheckpoint();

        try {
            while (!Thread.currentThread().isInterrupted())
                pages.add(allocatePage(memory));
        }
        catch (IgniteOutOfMemoryException ignore) {
            //Success
        }

        memory.finishCheckpoint();

        // Evict all previously allocated pages to PDS.
        memory.beginCheckpoint();

        try {
            while (!Thread.currentThread().isInterrupted())
                allocatePage(memory);
        }
        catch (IgniteOutOfMemoryException ignore) {
            //Success
        }

        memory.finishCheckpoint();

        // Acquire all pages allocated before eviction.
        memory.beginCheckpoint();

        for (FullPageId pageId : pages)
            memory.acquirePage(1, pageId.pageId());

        memory.finishCheckpoint();
    }

    /**
     * @param memory Memory.
     * @param fullPageId Full page id.
     * @throws IgniteCheckedException If acquiring lock failed.
     */
    private void acquireAndReleaseWriteLock(PageMemoryImpl memory, FullPageId fullPageId) throws IgniteCheckedException {
        long page = memory.acquirePage(1, fullPageId.pageId());

        long address = memory.writeLock(1, fullPageId.pageId(), page);

        PageIO.setPageId(address, fullPageId.pageId());

        PageIO.setType(address, PageIO.T_BPLUS_META);

        PageUtils.putShort(address, PageIO.VER_OFF, (short)1);

        memory.writeUnlock(1, fullPageId.pageId(), page, Boolean.FALSE, true);

        memory.releasePage(1, fullPageId.pageId(), page);
    }

    /**
     * @param throttlingPlc Throttling Policy.
     * @throws Exception If creating mock failed.
     */
    private PageMemoryImpl createPageMemory(PageMemoryImpl.ThrottlingPolicy throttlingPlc) throws Exception {
        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);

        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        igniteCfg.setDataStorageConfiguration(new DataStorageConfiguration());
        igniteCfg.setFailureHandler(new NoOpFailureHandler());

        GridTestKernalContext kernalCtx = new GridTestKernalContext(new GridTestLog4jLogger(), igniteCfg);
        kernalCtx.add(new IgnitePluginProcessor(kernalCtx, igniteCfg, Collections.<PluginProvider>emptyList()));

        FailureProcessor failureProc = new FailureProcessor(kernalCtx);

        failureProc.start();

        kernalCtx.add(failureProc);

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            kernalCtx,
            null,
            null,
            null,
            new NoOpPageStoreManager(),
            new NoOpWALManager(),
            null,
            new IgniteCacheDatabaseSharedManager(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        CheckpointWriteProgressSupplier noThrottle = Mockito.mock(CheckpointWriteProgressSupplier.class);

        Mockito.when(noThrottle.currentCheckpointPagesCount()).thenReturn(1_000_000);
        Mockito.when(noThrottle.evictedPagesCntr()).thenReturn(new AtomicInteger(0));
        Mockito.when(noThrottle.syncedPagesCounter()).thenReturn(new AtomicInteger(1_000_000));
        Mockito.when(noThrottle.writtenPagesCounter()).thenReturn(new AtomicInteger(1_000_000));

        PageMemoryImpl mem = new PageMemoryImpl(
            provider,
            new long[] {MAX_SIZE / 4, MAX_SIZE / 4, MAX_SIZE / 4, MAX_SIZE / 4},
            sharedCtx,
            PAGE_SIZE,
            (fullPageId, byteBuf, tag) -> {},
            new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(Long page, FullPageId fullId, PageMemoryEx pageMem) {
                }
            }, new CheckpointLockStateChecker() {
                @Override public boolean checkpointLockIsHeldByThread() {
                    return true;
                }
            },
            new DataRegionMetricsImpl(igniteCfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration()),
            throttlingPlc,
            noThrottle
        );

        mem.start();

        return mem;
    }
}
