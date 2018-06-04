package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

/**
 *
 */
public abstract class AbstractWalDeltaConsistencyTest extends GridCommonAbstractTest {
    /** Plan:
     *      1. Disable checkpoints.
     *      2. Prepare.
     *      3. Trigger checkpoint.
     *      4. Dump LFS state.
     *      5. Test code.
     *      6. Trigger checkpoint.
     *      7. Apply & compare.
     */
    protected IgniteEx ignite;

    /** Page memory copy. */
    protected DirectMemoryPageSupport pageMemoryCp;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setName("dflt-plc"));

        cfg.setDataStorageConfiguration(memCfg);

        return cfg;
    }

    /**
     *
     */
    public final void testWalDeltaConsistency() throws Exception {
        cleanPersistenceDir();

        ignite = startGrid(0);

        injectWalMgr();

        pageMemoryCp = new DirectMemoryPageSupport(log,
            ignite.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration(),
            ignite.configuration().getDataStorageConfiguration().getPageSize());

        ignite.cluster().active(true);

        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context().database();

        // TODO: page replacement must fail test

        prepare();

        dbMgr.forceCheckpoint("Before PDS dump").finishFuture().get();

        // TODO: disable checkpoint in beginFuture listener

        dbMgr.enableCheckpoints(false).get();

        // TODO: Dump LFS (DataRegion?) inside listener

        process();

        // Enable checkpoint - forces checkpoint.
        dbMgr.enableCheckpoints(true).get();

        dbMgr.forceCheckpoint("After process").finishFuture().get();

        dbMgr.enableCheckpoints(false).get();

/*
        try (WALIterator it = ignite.context().cache().context().wal().replay(null)) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it)
                applyWalRecord(tuple.getKey(), tuple.getValue());
        }
*/
    }

    /**
     *
     */
    public void prepare() {

    }

    /**
     *
     */
    public abstract void process();


    /**
     *
     */
    private void injectWalMgr() throws NoSuchFieldException, IllegalAccessException {
        IgniteWriteAheadLogManager walMgrOld = ignite.context().cache().context().wal();

        IgniteWriteAheadLogManager walMgrNew =  (IgniteWriteAheadLogManager) Proxy.newProxyInstance(
            IgniteWriteAheadLogManager.class.getClassLoader(),
            new Class[] {IgniteWriteAheadLogManager.class},
            new InvocationHandler() {
                @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                    try {
                        Object res = mtd.invoke(walMgrOld, args);

                        if ("log".equals(mtd.getName()) && args.length > 0 && args[0] instanceof WALRecord)
                            pageMemoryCp.applyWalRecord((WALPointer)res, (WALRecord)args[0]);

                        return res;
                    }
                    catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
                }
            }
        );

        // Inject new walMgr into GridCacheSharedContext
        Field walMgrFieldCtx = GridCacheSharedContext.class.getDeclaredField("walMgr");

        walMgrFieldCtx.setAccessible(true);

        walMgrFieldCtx.set(ignite.context().cache().context(), walMgrNew);

        // Inject new walMgr into each PageMemoryImpl
        Field walMgrFieldPageMem = PageMemoryImpl.class.getDeclaredField("walMgr");

        walMgrFieldPageMem.setAccessible(true);

        for (DataRegion dataRegion : ignite.context().cache().context().database().dataRegions()) {
            if (dataRegion.pageMemory() instanceof PageMemoryImpl)
                walMgrFieldPageMem.set(dataRegion.pageMemory(), walMgrNew);
        }
    }

    /**
     *
     */
    private static class DirectMemoryPageSupport {
        /** Memory region. */
        private final DirectMemoryRegion memoryRegion;

        /** Last allocated page index. */
        private final AtomicInteger lastPageIdx = new AtomicInteger();

        /** Pages. */
        private final Map<PageKey, DirectMemoryPage> pages = new ConcurrentHashMap<>();

        /** Page size. */
        private final int pageSize;

        /** Max pages. */
        private final int maxPages;

        /** Page memory mock. */
        private final PageMemory pageMemoryMock;

        /**
         * @param log Logger.
         * @param dataRegionCfg Data region configuration.
         * @param pageSize Page size.
         */
        public DirectMemoryPageSupport(IgniteLogger log, DataRegionConfiguration dataRegionCfg, int pageSize) {
            DirectMemoryProvider memProvider = new UnsafeMemoryProvider(log);

            long[] chunks = new long[] { dataRegionCfg.getMaxSize() };

            memProvider.initialize(chunks);

            memoryRegion = memProvider.nextRegion();

            this.pageSize = pageSize;

            maxPages = (int)(dataRegionCfg.getMaxSize() / pageSize);

            pageMemoryMock = Mockito.mock(PageMemory.class);

            Mockito.doReturn(pageSize).when(pageMemoryMock).pageSize();
        }

        /**
         * @param pageId Page id.
         */
        private synchronized DirectMemoryPage allocatePage(int grpId, long pageId) {
            // Double check.
            DirectMemoryPage page = pages.get(pageKey(grpId, pageId));

            if (page != null)
                return page;

            page = new DirectMemoryPage(lastPageIdx.getAndIncrement());

            if (page.pageIndex() >= maxPages)
                throw new IgniteException("Can't allocate new page");

            pages.put(pageKey(grpId, pageId), page);

            return page;
        }

        /**
         *
         * @param grpId Group id.
         * @param pageId Page id.
         * @return Page.
         */
        private DirectMemoryPage page(int grpId, long pageId) {
            DirectMemoryPage page = pages.get(pageKey(grpId, pageId)); // TODO

            if (page == null)
                page = allocatePage(grpId, pageId);

            return page;
        }

        /**
         * @param grpId Group id.
         * @param pageId Page id.
         */
        private static PageKey pageKey(int grpId, long pageId) {
            return new PageKey(grpId, PageIdUtils.effectivePageId(pageId));
        }
        /**
         * @param pageId Page id.
         */
        public long acquirePage(int grpId, long pageId) throws IgniteCheckedException {
            DirectMemoryPage page = page(grpId, pageId);

            page.lock().lock();

            return memoryRegion.address() + ((long)page.pageIndex()) * pageSize;
        }

        /**
         * @param pageId Page id.
         */
        public void releasePage(int grpId, long pageId) {
            DirectMemoryPage page = page(grpId, pageId);

            page.lock().unlock();
        }

        /**
         *
         */
        public void applyWalRecord(WALPointer pointer, WALRecord record) throws IgniteCheckedException {
            if (record instanceof PageSnapshot) {
                PageSnapshot snapshot = (PageSnapshot)record;

                int grpId = snapshot.fullPageId().groupId();
                long pageId = snapshot.fullPageId().pageId();

                long pageAddr = acquirePage(grpId, pageId);

                try {
                    PageUtils.putBytes(pageAddr, 0, snapshot.pageData());
                }
                finally {
                    releasePage(grpId, pageId);
                }
            }
            else if (record instanceof PageDeltaRecord) {
                PageDeltaRecord deltaRecord = (PageDeltaRecord)record;

                int grpId = deltaRecord.groupId();
                long pageId = deltaRecord.pageId();

                long pageAddr = acquirePage(grpId, pageId);

                try {
                    deltaRecord.applyDelta(pageMemoryMock, pageAddr);
                }
                finally {
                    releasePage(grpId, pageId);
                }
            }
        }

        /**
         *
         */
        private static class DirectMemoryPage {
            /** Page index. */
            private final int pageIdx;

            /** Page lock. */
            private final Lock lock = new ReentrantLock();

            /**
             * @param idx Page index.
             */
            private DirectMemoryPage(int idx) {
                pageIdx = idx;
            }

            /**
             * @return Page lock.
             */
            public Lock lock() {
                return lock;
            }

            /**
             * @return Page index.
             */
            public int pageIndex() {
                return pageIdx;
            }
        }

        /**
         *
         */
        private static class PageKey {
            /** Group id. */
            private final int grpId;

            /** Effective page id. */
            private final long effPageId;

            /**
             * @param grpId Group id.
             * @param effPageId Eff page id.
             */
            private PageKey(int grpId, long effPageId) {
                this.grpId = grpId;
                this.effPageId = effPageId;
            }

            /** {@inheritDoc} */
            @Override public int hashCode() {
                return PageIdUtils.partId(effPageId) << 16 ^ PageIdUtils.pageIndex(effPageId) ^ grpId;
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object obj) {
                if (obj instanceof PageKey)
                    return ((PageKey)obj).effPageId == this.effPageId && ((PageKey)obj).grpId == this.grpId;

                return false;
            }
        }
    }
}
