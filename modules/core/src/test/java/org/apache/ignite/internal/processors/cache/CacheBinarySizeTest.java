package org.apache.ignite.internal.processors.cache;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.CacheVersionIO;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class CacheBinarySizeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(
            new CacheConfiguration()
                .setCacheMode(CacheMode.PARTITIONED)
                .setName(DEFAULT_CACHE_NAME)
                .setGroupName("grp")
                .setBackups(1)
            )
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setName("dataRegion")
                            .setPersistenceEnabled(true)
                            .setMetricsEnabled(true)
                    ).setMetricsEnabled(true)
            );
    }

    /**
     *
     */
    public void testAllCachesSize() throws Exception {
        cleanPersistenceDir();

        startGrids(4);

        grid(0).cluster().active(true);

        for (Ignite ignite : G.allGrids()) {
            IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

            for (Integer k : primaryKeys(cache, 1)) {
                cache.put(new TestKey(k), new TestValue(k));
                cache.put(k, k);
                cache.put("string" + k, "" + k);
                cache.put("integer[]" + k, new Integer[] {k});
                cache.put("string[]" + k, new String[] {"" + k});
                cache.put("object[]" + k, new Object[] {new Object()});
            }
        }

        long sampleSize = Long.MAX_VALUE;

        for (Ignite ignite : G.allGrids()) {
            NodeSizeInfo nodeSizeInfo = new NodeSizeInfo(ignite.cluster().localNode().id().toString());

            for (CacheGroupContext grp : ((IgniteEx)ignite).context().cache().cacheGroups()) {
                long grpSizeBytes = grp.mxBean().getTotalAllocatedSize();

                CacheGroupSizeInfo cacheGrpSizeInfo = new CacheGroupSizeInfo(grp.cacheOrGroupName(), grpSizeBytes);
                nodeSizeInfo.addCacheGroupSize(cacheGrpSizeInfo);

                log.info("Grid: " + ignite.configuration().getIgniteInstanceName()
                    + " grp: " + grp.cacheOrGroupName()
                    + " size: " + grpSizeBytes
                );

                for (GridCacheContext cctx : grp.caches()) {
                    long cacheSize = cctx.cache().localSizeLong(new CachePeekMode[] {
                        CachePeekMode.OFFHEAP, CachePeekMode.PRIMARY, CachePeekMode.BACKUP});

                    Iterator<CacheDataRow> it = cctx.offheap().cacheIterator(
                        cctx.cacheId(),
                        true,
                        true,
                        null);

                    long cacheSizeBytes = 0;
                    long entriesCnt = 0;

                    while (it.hasNext() && entriesCnt < sampleSize) {
                        entriesCnt++;
                        CacheDataRow row = it.next();

                        int rowSize = 4 + 1 + 4 + 1 + 8; // Key size (int) + key type (byte) + value size (int) + value type (byte) + expire time (long)
                        rowSize += ObjectSizeExtractor.getSize(row.key());
                        rowSize += ObjectSizeExtractor.getSize(row.value());
                        rowSize += CacheVersionIO.size(row.version(), false);

                        cacheSizeBytes += rowSize;

                        //log.info("Row size: " + rowSize + " row: " + row);
                    }

                    cacheGrpSizeInfo.addCacheSize(new CacheSizeInfo(cctx.name(), cacheSizeBytes, entriesCnt, cacheSize));

                    //log.info("Cache: " + cctx.name() + " size: " + cacheSizeBytes + " count: " + cacheSize);
                }
                cacheGrpSizeInfo.estimateCachesSize();
            }

            nodeSizeInfo.dump(System.out);
        }

        stopAllGrids();
    }

    /**
     *
     */
    public void testCacheSize() throws Exception {
        cleanPersistenceDir();

        startGrids(4);

        grid(0).cluster().active(true);

        for (Ignite ignite : G.allGrids()) {
            IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);
            for (Integer k : primaryKeys(cache, 5000)) {
                cache.put(new TestKey(k), new TestValue(k));
                cache.put(k, k);
                cache.put("string" + k, "" + k);
                cache.put("integer[]" + k, new Integer[] {k});
                cache.put("string[]" + k, new String[] {"" + k});
                cache.put("object[]" + k, new Object[] {new Object()});
            }
        }

        for (Ignite ignite : G.allGrids()) {
            IgniteInternalCache internalCache = ((IgniteEx)ignite).cachex(DEFAULT_CACHE_NAME);

            long cacheSize = internalCache.localSizeLong(new CachePeekMode[] {CachePeekMode.OFFHEAP, CachePeekMode.PRIMARY, CachePeekMode.BACKUP});

            log.info("Cache size: " + cacheSize);

            Iterator<CacheDataRow> it = internalCache.context().offheap().cacheIterator(
                CU.cacheId(DEFAULT_CACHE_NAME),
                true,
                true,
                null);

            while (it.hasNext()) {
                CacheDataRow row = it.next();

                int size = 4+1+4+1+8; // Key size (int) + key type (byte) + value size (int) + value type (byte) + expire time (long)
                size += ObjectSizeExtractor.getSize(row.key());
                size += ObjectSizeExtractor.getSize(row.value());
                size += CacheVersionIO.size(row.version(), false);

                log.info("Row size: " + size + " row: " + row);
            }

            IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME).withKeepBinary();

            Iterable<Cache.Entry> iterable = cache.localEntries(CachePeekMode.PRIMARY, CachePeekMode.BACKUP);

            for (Cache.Entry entry : iterable) {
                Object key = entry.getKey();
                Object val = entry.getValue();
                log.info("Key: " + key + " Val: " + val);
            }
        }

        stopAllGrids();
    }

    /**
     *
     */
    public static class TestKey {
        /** Key int. */
        Integer keyInt;
        /** Key string. */
        String keyStr;

        /**
         * @param keyInt Key int.
         */
        public TestKey(Integer keyInt) {
            this.keyInt = keyInt;
            this.keyStr = "Key" + keyInt;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return keyInt.hashCode();
        }
    }

    /**
     *
     */
    public static class TestValue {
        /** Value int. */
        Integer valInt;
        /** Value string. */
        String valStr;
        /** Buffer. */
        byte[] buf;

        /**
         * @param valInt Value int.
         */
        public TestValue(Integer valInt) {
            this.valInt = valInt;
            this.valStr = "Value: " + valInt;
            this.buf = new byte[valInt];
        }
    }

    /**
     *
     */
    private static class ObjectSizeExtractor extends CacheObjectImpl {
        /**
         * @param obj Object.
         */
        public static int getSize(CacheObject obj) {
            try {
                return obj.valueBytesLength(null);
/*
                if (obj instanceof CacheObjectAdapter && ((CacheObjectAdapter)obj).valBytes != null)
                    return ((CacheObjectAdapter)obj).valBytes.length;
                else if (obj instanceof BinaryObjectExImpl && ((BinaryObjectExImpl)obj).array() != null)
                    return ((BinaryObjectExImpl)obj).array().length;
                else if (obj instanceof BinaryEnumObjectImpl)
                    return obj.valueBytes(null).length;
                else
                    return 0;
*/
            }
            catch (Exception ignore) {
                System.out.println("error: " + ignore);
                return 0;
            }
        }
    }

    private static class NodeSizeInfo {
        String nodeId;
        Collection<CacheGroupSizeInfo> cacheGrpSizeInfos = new LinkedList<>();

        public NodeSizeInfo(String nodeId) {
            this.nodeId = nodeId;
        }

        public void addCacheGroupSize(CacheGroupSizeInfo cacheGrpSizeInfo) {
            cacheGrpSizeInfos.add(cacheGrpSizeInfo);
        }

        public void dump(PrintStream stream) {
            stream.println("Node id;Cache group name;Cache name;Sample size;Sample count; Total count;Estimated size");

            for (CacheGroupSizeInfo cacheGrpSizeInfo : cacheGrpSizeInfos)
                for (CacheSizeInfo cacheSizeInfo : cacheGrpSizeInfo.cacheSizeInfos)
                    stream.println(nodeId
                        + ';' + cacheGrpSizeInfo.cacheGrpName
                        + ';' + cacheSizeInfo.cacheName
                        + ';' + cacheSizeInfo.sampleSize
                        + ';' + cacheSizeInfo.sampleCount
                        + ';' + cacheSizeInfo.totalCount
                        + ';' + cacheSizeInfo.estimatedSize
                    );
        }
    }

    private static class CacheGroupSizeInfo {
        String cacheGrpName;
        long cacheGrpSize;
        long cachesDataSize = 0;

        Collection<CacheSizeInfo> cacheSizeInfos = new LinkedList<>();

        public CacheGroupSizeInfo(String cacheGrpName, long cacheGrpSize) {
            this.cacheGrpName = cacheGrpName;
            this.cacheGrpSize = cacheGrpSize;
        }

        public void estimateCachesSize() {
            if (cacheGrpSize != 0 && cachesDataSize != 0) {
                for (CacheSizeInfo cacheSizeInfo : cacheSizeInfos) {
                    // Using long arithmetic with 0.001 precession
                    long estimatedOverheadCoef = 1000 * cacheGrpSize / cachesDataSize;
                    cacheSizeInfo.estimatedSize = cacheSizeInfo.dataSize * estimatedOverheadCoef / 1000;
                }
            }
        }

        public void addCacheSize(CacheSizeInfo cacheSizeInfo) {
            cacheSizeInfos.add(cacheSizeInfo);

            cachesDataSize += cacheSizeInfo.dataSize;
        }
    }

    private static class CacheSizeInfo {
        String cacheName;
        long sampleSize;
        long sampleCount;
        long totalCount;
        long dataSize;

        long estimatedSize;

        public CacheSizeInfo(String cacheName, long sampleSize, long sampleCnt, long totalCnt) {
            this.cacheName = cacheName;
            this.sampleSize = sampleSize;
            this.sampleCount = sampleCnt;
            this.totalCount = totalCnt;

            if (sampleSize > 0)
                dataSize = sampleCnt == totalCnt ? sampleSize : sampleSize / sampleCnt * totalCnt;
        }

    }
}
