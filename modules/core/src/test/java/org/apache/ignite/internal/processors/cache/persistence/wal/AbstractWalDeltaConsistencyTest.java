package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.apache.ignite.internal.util.GridUnsafe;
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

        pageMemoryCp.compareTo(dbMgr.dataRegion("dflt-plc"));

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

            int pageIdx = lastPageIdx.getAndIncrement();

            if (pageIdx >= maxPages)
                throw new IgniteException("Can't allocate new page");

            long pageAddr = memoryRegion.address() + ((long)pageIdx) * pageSize;

            page = new DirectMemoryPage(pageAddr);

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
            return new PageKey(grpId, pageId);
        }

        /**
         *
         */
        public void applyWalRecord(WALPointer pointer, WALRecord record) throws IgniteCheckedException {
            if (record instanceof PageSnapshot) {
                PageSnapshot snapshot = (PageSnapshot)record;

                int grpId = snapshot.fullPageId().groupId();
                long pageId = snapshot.fullPageId().pageId();

                DirectMemoryPage page = page(grpId, pageId);

                page.lock();

                try {
                    PageUtils.putBytes(page.address(), 0, snapshot.pageData());

                    page.changeHistory().clear();

                    page.changeHistory().add(record);
                }
                finally {
                    page.unlock();
                }
            }
            else if (record instanceof PageDeltaRecord) {
                PageDeltaRecord deltaRecord = (PageDeltaRecord)record;

                int grpId = deltaRecord.groupId();
                long pageId = deltaRecord.pageId();

                DirectMemoryPage page = page(grpId, pageId);

                page.lock();

                try {
                    deltaRecord.applyDelta(pageMemoryMock, page.address());

                    page.changeHistory().add(record);

                    // Page corruptor TODO: remove
                    if (new Random().nextInt(2000) == 0)
                        GridUnsafe.putByte(page.address() + new Random().nextInt(pageSize),
                            (byte)(new Random().nextInt(256)));
                }
                finally {
                    page.unlock();
                }
            }
        }

        /**
         * @param dataRegion Data region.
         */
        public boolean compareTo(DataRegion dataRegion) throws IgniteCheckedException {
            for (PageKey pageKey : pages.keySet()) {
                long rmtPage = dataRegion.pageMemory().acquirePage(pageKey.groupId(), pageKey.pageId());

                try {
                    long rmtPageAddr = dataRegion.pageMemory().readLock(pageKey.groupId(), pageKey.pageId(), rmtPage);

                    try {
                        DirectMemoryPage page = page(pageKey.groupId(), pageKey.pageId());

                        page.lock();

                        try {
                            ByteBuffer locBuf = GridUnsafe.wrapPointer(page.address(), pageSize);
                            ByteBuffer rmtBuf = GridUnsafe.wrapPointer(rmtPageAddr, pageSize);

                            if (!locBuf.equals(rmtBuf))
                                System.out.println("Not equals page buffer for grpId: " + pageKey.groupId() + ",  pageId: " + pageKey.pageId());
                        }
                        finally {
                            page.unlock();
                        }
                    }
                    finally {
                        dataRegion.pageMemory().readUnlock(pageKey.groupId(), pageKey.pageId(), rmtPage);
                    }
                }
                finally {
                    dataRegion.pageMemory().releasePage(pageKey.groupId(), pageKey.pageId(), rmtPage);
                }
            }

            return true;
        }

        /**
         *
         */
        private static class DirectMemoryPage {
            /** Page address. */
            private final long addr;

            /** Page lock. */
            private final Lock lock = new ReentrantLock();

            /** Change history. */
            private final List<WALRecord> changeHist = new LinkedList<>();

            /**
             * @param addr Page address.
             */
            private DirectMemoryPage(long addr) {
                this.addr = addr;
            }

            /**
             * Lock page.
             */
            public void lock() {
                lock.lock();
            }

            /**
             * Unlock page.
             */
            public void unlock() {
                lock.unlock();
            }

            /**
             * @return Page address.
             */
            public long address() {
                return addr;
            }

            /**
             * Change history.
             */
            public List<WALRecord> changeHistory() {
                return changeHist;
            }
        }

        /**
         * Page key.
         */
        private static class PageKey {
            /** Group id. */
            private final int grpId;

            /** Page id. */
            private final long pageId;

            /** Effective page id. */
            private final long effPageId;

            /**
             * @param grpId Group id.
             * @param pageId Page id.
             */
            private PageKey(int grpId, long pageId) {
                this.grpId = grpId;
                this.pageId = pageId;
                this.effPageId = PageIdUtils.effectivePageId(pageId);
            }

            /** {@inheritDoc} */
            @Override public int hashCode() {
                return PageIdUtils.partId(effPageId) << 16 ^ PageIdUtils.pageIndex(effPageId) ^ grpId;
            }

            /** {@inheritDoc} */
            @Override public boolean equals(Object obj) {
                return obj instanceof PageKey && ((PageKey)obj).effPageId == this.effPageId
                    && ((PageKey)obj).grpId == this.grpId;
            }

            /**
             * Page id.
             */
            public long pageId() {
                return pageId;
            }

            /**
             * Group id.
             */
            public int groupId() {
                return grpId;
            }
        }
    }
}
