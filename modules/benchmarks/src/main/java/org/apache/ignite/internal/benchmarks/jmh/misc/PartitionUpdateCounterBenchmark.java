package org.apache.ignite.internal.benchmarks.jmh.misc;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.benchmarks.jmh.runner.JmhIdeBenchmarkRunner;
import org.apache.ignite.internal.processors.cache.PartitionTxUpdateCounterImpl;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Benchmarks {@link PartitionTxUpdateCounterImpl} class.
 */
@State(Scope.Benchmark)
public class PartitionUpdateCounterBenchmark {
    /** Buffer size to store gaps. */
    private static final int GAPS_BUFFER_SIZE = 50;

    /** Max delta for next counter value. */
    private static final int COUNTER_MAX_DELTA= 50;

    /** Gaps buffer. */
    private final long [][] gapsBuf = new long[GAPS_BUFFER_SIZE][];

    /** Random numbers generator. */
    private Random rnd;

    /** Current counter. */
    private final AtomicLong curCntr = new AtomicLong();

    /** Partition update counter. */
    private final PartitionUpdateCounter partCntr = new PartitionTxUpdateCounterImpl();

    /**
     * Setup.
     */
    @Setup(Level.Iteration)
    public void setup() {
        rnd = new Random(0);

        curCntr.set(0);

        for (int i = 0; i < GAPS_BUFFER_SIZE; i++) {
            long cntrDelta = rnd.nextInt(COUNTER_MAX_DELTA);

            gapsBuf[i] = new long[] {curCntr.getAndAdd(cntrDelta), cntrDelta};
        }

        partCntr.reset();
    }

    /**
     * Update partition update counter with random gap.
     */
    @Benchmark
    public void updateWithGap() {
        int nextIdx = rnd.nextInt(GAPS_BUFFER_SIZE);

        partCntr.update(gapsBuf[nextIdx][0], gapsBuf[nextIdx][1]);

        gapsBuf[nextIdx][0] = curCntr.getAndAdd(gapsBuf[nextIdx][1]);
    }

    /**
     *
     * @param args Args.
     * @throws Exception Exception.
     */
    public static void main(String[] args) throws Exception {
        JmhIdeBenchmarkRunner.create()
            .threads(1)
            .measurementIterations(10)
            .warmupIterations(5)
            .benchmarkModes(Mode.Throughput)
            .outputTimeUnit(TimeUnit.MILLISECONDS)
            .benchmarks(PartitionUpdateCounterBenchmark.class.getSimpleName())
            .run();
    }
}
