package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.DirectMemoryRegion;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageSupport;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class AbstractWalDeltaConsistencyTest extends GridCommonAbstractTest {
    /** Page size. */
    private static final int PAGE_SIZE = 2048;

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

        Field walMgrField = GridCacheSharedContext.class.getDeclaredField("walMgr");

        walMgrField.setAccessible(true);

        walMgrField.set(ignite.context().cache().context(), walMgrNew);
    }

    /**
     *
     */
    private static class DirectMemoryPageSupport implements PageSupport {
        /** Memory region. */
        DirectMemoryRegion memoryRegion;

        /** Page size. */
        int pageSize;

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
        }

        /** {@inheritDoc} */
        @Override public long acquirePage(int grpId, long pageId) throws IgniteCheckedException {
            return memoryRegion.address() + PageIdUtils.pageIndex(pageId) * pageSize;
        }

        /** {@inheritDoc} */
        @Override public void releasePage(int grpId, long pageId, long page) {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public long readLock(int grpId, long pageId, long page) {
            return page;
        }

        /** {@inheritDoc} */
        @Override public long readLockForce(int grpId, long pageId, long page) {
            return page;
        }

        /** {@inheritDoc} */
        @Override public void readUnlock(int grpId, long pageId, long page) {

        }

        /** {@inheritDoc} */
        @Override public long writeLock(int grpId, long pageId, long page) {
            return page;
        }

        /** {@inheritDoc} */
        @Override public long tryWriteLock(int grpId, long pageId, long page) {
            return page;
        }

        /** {@inheritDoc} */
        @Override public void writeUnlock(int grpId, long pageId, long page, Boolean walPlc, boolean dirtyFlag) {

        }

        /** {@inheritDoc} */
        @Override public boolean isDirty(int grpId, long pageId, long page) {
            return false;
        }

        /**
         *
         */
        public void applyWalRecord(WALPointer pointer, WALRecord record) throws IgniteCheckedException {
            if (record instanceof PageSnapshot) {
                PageSnapshot snapshot = (PageSnapshot)record;

                int grpId = snapshot.fullPageId().groupId();
                long pageId = snapshot.fullPageId().pageId();

                long curPage = acquirePage(grpId, pageId);

                try {
                    long curAddr = readLock(grpId, pageId, curPage);

                    try {
                        PageUtils.putBytes(curAddr, 0, snapshot.pageData());
                    }
                    finally {
                        readUnlock(grpId, pageId, curPage);
                    }
                }
                finally {
                    releasePage(grpId, pageId, curPage);
                }
            }
            else if (record instanceof PageDeltaRecord) {
                PageDeltaRecord deltaRecord = (PageDeltaRecord)record;

                int grpId = deltaRecord.groupId();
                long pageId = deltaRecord.pageId();

                long curPage = acquirePage(grpId, pageId);

                try {
                    long curAddr = readLock(grpId, pageId, curPage);

                    try {
                        deltaRecord.applyDelta(pageMemoryCp, curAddr);
                    }
                    finally {
                        readUnlock(grpId, pageId, curPage);
                    }
                }
                finally {
                    releasePage(grpId, pageId, curPage);
                }
            }
        }
    }
}

