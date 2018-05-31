package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.file.MappedFileMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.impl.PageMemoryNoStoreImpl;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.util.GridUnsafe.wrapPointer;

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
    protected PageMemory pageMemoryCp;

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
    private PageMemory pageMemoryCopy() {
        DataStorageConfiguration dataStorageCfg = ignite.configuration().getDataStorageConfiguration();
        DataRegionConfiguration dataRegionCfg = dataStorageCfg.getDefaultDataRegionConfiguration();

        DataRegionMetricsImpl memMetrics = new DataRegionMetricsImpl(dataRegionCfg, new IgniteOutClosure<Long>() {
            @Override public Long apply() {
                return null;
            }
        });

        File allocPath = null; //buildAllocPath(plcCfg);

        DirectMemoryProvider memProvider = allocPath == null ?
            new UnsafeMemoryProvider(log) :
            new MappedFileMemoryProvider(
                log,
                allocPath);

        PageMemory pageMem = new PageMemoryNoStoreImpl(
            log,
            memProvider,
            ignite.context().cache().context(),
            dataStorageCfg.getPageSize(),
            dataRegionCfg,
            memMetrics,
            false
        );

        pageMem.start();

        return pageMem;
    }

    /**
     *
     */
    public final void testWalDeltaConsistency() throws Exception {
        cleanPersistenceDir();

        ignite = startGrid(0);

        injectWalMgr();

        pageMemoryCp = pageMemoryCopy();

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

        try (WALIterator it = ignite.context().cache().context().wal().replay(null)) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it)
                applyWalRecord(tuple.getKey(), tuple.getValue());
        }
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
    private void applyWalRecord(WALPointer pointer, WALRecord record) throws IgniteCheckedException {
        if (record instanceof PageSnapshot) {
            PageSnapshot snapshot = (PageSnapshot)record;

            int grpId = snapshot.fullPageId().groupId();
            long pageId = snapshot.fullPageId().pageId();

            long curPage = pageMemoryCp.acquirePage(grpId, pageId);

            try {
                long curAddr = pageMemoryCp.readLock(grpId, pageId, curPage);

                try {
                    PageUtils.putBytes(curAddr, 0, snapshot.pageData());
                }
                finally {
                    pageMemoryCp.readUnlock(grpId, pageId, curPage);
                }
            }
            catch (Throwable t) {
                t.printStackTrace();
            }
            finally {
                pageMemoryCp.releasePage(grpId, pageId, curPage);
            }
        }
        else if (record instanceof PageDeltaRecord) {
            PageDeltaRecord deltaRecord = (PageDeltaRecord)record;

            int grpId = deltaRecord.groupId();
            long pageId = deltaRecord.pageId();

            long curPage = pageMemoryCp.acquirePage(grpId, pageId);

            try {
                long curAddr = pageMemoryCp.readLock(grpId, pageId, curPage);

                try {
                    deltaRecord.applyDelta(pageMemoryCp, curAddr);
                }
                finally {
                    pageMemoryCp.readUnlock(grpId, pageId, curPage);
                }
            }
            catch (Throwable t) {
                t.printStackTrace();
            }
            finally {
                pageMemoryCp.releasePage(grpId, pageId, curPage);
            }
        }
    }

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
                            applyWalRecord((WALPointer)res, (WALRecord)args[0]);

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
}
