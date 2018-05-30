package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.nio.ByteBuffer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.mockito.Mockito;

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

        Long tmpAddr = null;
        ByteBuffer curPage = null;
        FullPageId fullId = null;
        PageMemoryImpl pageMemory = Mockito.mock(PageMemoryImpl.class);
        Mockito.doReturn(PAGE_SIZE).when(pageMemory).pageSize();

        try (WALIterator it = ignite.context().cache().context().wal().replay(null)) {
            for (IgniteBiTuple<WALPointer, WALRecord> tuple : it) {
                switch (tuple.getValue().type()) {
                    case PAGE_RECORD:
                        PageSnapshot snapshot = (PageSnapshot)tuple.getValue();

                        if (snapshot.fullPageId().equals(fullId)) {
                            if (tmpAddr == null) {
                                assert snapshot.pageData().length <= PAGE_SIZE : snapshot.pageData().length;

                                tmpAddr = GridUnsafe.allocateMemory(PAGE_SIZE);
                            }

                            if (curPage == null)
                                curPage = wrapPointer(tmpAddr, PAGE_SIZE);

                            PageUtils.putBytes(tmpAddr, 0, snapshot.pageData());
                        }

                        break;

                    default:
                        if (tuple.getValue() instanceof PageDeltaRecord) {
                            PageDeltaRecord deltaRecord = (PageDeltaRecord)tuple.getValue();

                            if (curPage != null
                                && deltaRecord.pageId() == fullId.pageId()
                                && deltaRecord.groupId() == fullId.groupId()) {
                                assert tmpAddr != null;

                                deltaRecord.applyDelta(pageMemory, tmpAddr);
                            }
                        }
                }
            }
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
}
