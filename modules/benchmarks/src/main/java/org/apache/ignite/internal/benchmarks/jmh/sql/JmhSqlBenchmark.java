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

package org.apache.ignite.internal.benchmarks.jmh.sql;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark simple SQL queries.
 */
@State(Scope.Benchmark)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
public class JmhSqlBenchmark {
    /** Count of server nodes. */
    private static final int SRV_NODES_CNT = 3;

    /** Keys count. */
    private static final int KEYS_CNT = 100000;

    /** Size of batch. */
    private static final int BATCH_SIZE = 1000;

    /** Partitions count. */
    private static final int PARTS_CNT = 1024;

    /** IP finder shared across nodes. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Query engine. */
    @Param({"H2", "CALCITE"})
    private String engine;

    /** Ignite client. */
    private Ignite client;

    /** Servers. */
    private final Ignite[] servers = new Ignite[SRV_NODES_CNT];

    /** Cache. */
    private IgniteCache<Integer, Item> cache;

    /**
     * Create Ignite configuration.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Configuration.
     */
    private IgniteConfiguration configuration(String igniteInstanceName) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);
        cfg.setLocalHost("127.0.0.1");
        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        return cfg;
    }

    /**
     * Initiate Ignite and caches.
     */
    @Setup(Level.Trial)
    public void setup() {
        if ("CALCITE".equals(engine))
            System.setProperty(IgniteSystemProperties.IGNITE_EXPERIMENTAL_SQL_ENGINE, "true");
        else
            System.clearProperty(IgniteSystemProperties.IGNITE_EXPERIMENTAL_SQL_ENGINE);

        for (int i = 0; i < SRV_NODES_CNT; i++)
            servers[i] = Ignition.start(configuration("server" + i));

        client = Ignition.start(configuration("client").setClientMode(true));

        cache = client.getOrCreateCache(new CacheConfiguration<Integer, Item>("CACHE")
            .setIndexedTypes(Integer.class, Item.class)
            .setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT))
        );

        try (IgniteDataStreamer<Integer, Item> ds = client.dataStreamer("CACHE")) {
            for (int i = 0; i < KEYS_CNT; i++)
                ds.addData(i, new Item(i));
        }
    }

    /**
     * Stop Ignite instance.
     */
    @TearDown
    public void tearDown() {
        client.close();

        for (Ignite ignite : servers)
            ignite.close();
    }

    /**
     * Query unique value (full scan).
     */
    @Benchmark
    public void querySimpleUnique() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<?> res = executeSql("SELECT name FROM Item WHERE fld=?", key);

        assert res.size() == 1;
    }

    /**
     * Query unique value (indexed).
     */
    @Benchmark
    public void querySimpleUniqueIndexed() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<?> res = executeSql("SELECT name FROM Item WHERE fldIdx=?", key);

        assert res.size() == 1;
    }

    /**
     * Query batch (full scan).
     */
    @Benchmark
    public void querySimpleBatch() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<?> res = executeSql("SELECT name FROM Item WHERE fldBatch=?", key / BATCH_SIZE);

        assert res.size() == BATCH_SIZE;
    }

    /**
     * Query batch (indexed).
     */
    @Benchmark
    public void querySimpleBatchIndexed() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<?> res = executeSql("SELECT name FROM Item WHERE fldIdxBatch=?", key / BATCH_SIZE);

        assert res.size() == BATCH_SIZE;
    }

    /**
     * Query with group by and aggregate.
     */
    @Benchmark
    public void queryGroupBy() {
        List<?> res = executeSql("SELECT fldBatch, AVG(fld) FROM Item GROUP BY fldBatch");

        assert res.size() == KEYS_CNT / BATCH_SIZE;
    }

    /**
     * Query with indexed field group by and aggregate.
     */
    @Benchmark
    public void queryGroupByIndexed() {
        List<?> res = executeSql("SELECT fldIdxBatch, AVG(fld) FROM Item GROUP BY fldIdxBatch");

        assert res.size() == KEYS_CNT / BATCH_SIZE;
    }

    /**
     * Query with sorting (full set).
     */
    @Benchmark
    public void queryOrderByFull() {
        List<?> res = executeSql("SELECT name, fld FROM Item ORDER BY fld DESC");

        assert res.size() == KEYS_CNT;
    }

    /**
     * Query with sorting (batch).
     */
    @Benchmark
    public void queryOrderByBatch() {
        int key = ThreadLocalRandom.current().nextInt(KEYS_CNT);

        List<?> res = executeSql("SELECT name, fld FROM Item WHERE fldIdxBatch=? ORDER BY fld DESC", key / BATCH_SIZE);

        assert res.size() == BATCH_SIZE;
    }

    /** */
    private List<?> executeSql(String sql, Object... args) {
        List<List<?>> res = cache.query(new SqlFieldsQuery(sql).setArgs(args)).getAll();

        return res.get(0);
    }

    /**
     * Run benchmarks.
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        final Options options = new OptionsBuilder()
            .include(JmhSqlBenchmark.class.getSimpleName())
            .build();

        new Runner(options).run();
    }

    /** */
    private static class Item {
        /** */
        @QuerySqlField
        private final String name;

        /** */
        @QuerySqlField
        private final int fld;

        /** */
        @QuerySqlField
        private final int fldBatch;

        /** */
        @QuerySqlField(index = true)
        private final int fldIdx;

        /** */
        @QuerySqlField(index = true)
        private final int fldIdxBatch;

        /** */
        public Item(int val) {
            name = "name" + val;
            fld = val;
            fldBatch = val / BATCH_SIZE;
            fldIdx = val;
            fldIdxBatch = val / BATCH_SIZE;
        }
    }
}
