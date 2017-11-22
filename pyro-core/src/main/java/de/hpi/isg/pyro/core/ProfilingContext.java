package de.hpi.isg.pyro.core;

import com.google.common.util.concurrent.AtomicDouble;
import de.hpi.isg.pyro.model.*;
import de.hpi.isg.pyro.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * This class contains the relevant data to operate Pyro within a JVM.
 */
public class ProfilingContext extends DependencyConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The configuration of Pyro.
     */
    public final Configuration configuration;

    /**
     * Caches {@link PositionListIndex}es.
     */
    public final PLICache pliCache;

    /**
     * Caches {@link AgreeSetSample}s.
     */
    public final VerticalMap<AgreeSetSample> agreeSetSamples;

    /**
     * The {@link ColumnLayoutRelationData} to be profiled.
     */
    public final ColumnLayoutRelationData relationData;

    /**
     * Provides randomness.
     */
    public final Random random;

    /**
     * Collects performance profiling data.
     */
    public final ProfilingData profilingData = new ProfilingData();

    public final MemoryWatchdog memoryWatchdog = MemoryWatchdog.start(0.85, 100);

    /**
     * Creates a new instance.
     *
     * @param configuration the configuration for Pyro
     * @param relationData  that should be profiled
     * @param executor
     */
    public ProfilingContext(Configuration configuration,
                            ColumnLayoutRelationData relationData,
                            Consumer<PartialKey> uccConsumer,
                            Consumer<PartialFD> fdConsumer, Parallel.Executor executor) {
        final long startMillis = System.currentTimeMillis();
        this.configuration = configuration;
        this.relationData = relationData;
        this.uccConsumer = uccConsumer;
        this.fdConsumer = fdConsumer;
        this.random = this.configuration.seed == null ? new Random() : new Random(this.configuration.seed);
        this.pliCache = new PLICache(
                relationData,
                true
        );
        this.memoryWatchdog.addListener(() -> {
            VerticalMap<PositionListIndex> index = this.pliCache.getIndex();
            int initialCacheSize = index.size();
            long elapsedNanos = System.nanoTime();
            index.shrink(
                    0.5d,
                    Comparator.comparingInt(entry -> entry.getKey().getArity()),
                    entry -> entry.getKey().getArity() > 1
            );
            elapsedNanos = System.nanoTime() - elapsedNanos;
            logger.info(String.format("Shrinked PLI cache size from %,d to %,d in %,d ms.",
                    initialCacheSize, index.size(), elapsedNanos / 1_000_000
            ));
        });

        if (configuration.sampleSize > 0) {
            // Create the initial samples.
            RelationSchema schema = this.relationData.getSchema();
            this.agreeSetSamples = new SynchronizedVerticalMap<>(schema);
            this.memoryWatchdog.addListener(() -> {
                int initialCacheSize = this.agreeSetSamples.size();
                long elapsedNanos = System.nanoTime();
                this.agreeSetSamples.shrink(
                        0.5d,
                        Comparator.comparingInt(entry -> entry.getKey().getArity()),
                        entry -> entry.getKey().getArity() > 1
                );
                elapsedNanos = System.nanoTime() - elapsedNanos;
                logger.info(String.format("Shrinked agree set sample cache size from %,d to %,d in %,d ms.",
                        initialCacheSize, this.agreeSetSamples.size(), elapsedNanos / 1_000_000
                ));
            });
            Parallel.forEach(
                    schema.getColumns(),
                    column -> {
                        AgreeSetSample sample = this.createFocusedSample(column, 1d);
                        this.agreeSetSamples.put(column, sample);
                    },
                    executor,
                    true
            );

        } else {
            this.agreeSetSamples = null;
        }

        // Request garbage collection so that we do not end up clearing the caches while keeping all sorts of short-lived garbage.
        this.memoryWatchdog.addListener(System::gc);

        this.profilingData.initializationMillis.addAndGet(System.currentTimeMillis() - startMillis);
    }

    public void createColumnAgreeSetSamples(Function<Runnable, Future<?>> exeutor) {
        List<Future<?>> futures = new ArrayList<>();
        for (Column column : this.relationData.getSchema().getColumns()) {
            futures.add(exeutor.apply(() -> {
                AgreeSetSample sample = this.createFocusedSample(column, 1d);
                this.agreeSetSamples.put(column, sample);
            }));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Create and cache an {@link AgreeSetSample}.
     *
     * @param focus       the sampling focus
     * @param boostFactor the sampling boost (cf. {@link Configuration#sampleSize} and {@link Configuration#sampleBooster})
     * @return the created {@link AgreeSetSample}
     */
    AgreeSetSample createFocusedSample(Vertical focus, double boostFactor) {
        final long startNanos = System.nanoTime();
        ListAgreeSetSample sample = ListAgreeSetSample.createFocusedFor(
                this.relationData,
                focus,
                this.pliCache.getOrCreateFor(focus, this),
                (int) (this.configuration.sampleSize * boostFactor),
                this.random
        );
        if (logger.isDebugEnabled()) logger.debug("Created {} with a boost factor of {}.", sample, boostFactor);
        this.agreeSetSamples.put(focus, sample);
        this.profilingData.samplingNanos.addAndGet(System.nanoTime() - startNanos);
        this.profilingData.numSamplings.incrementAndGet();
        return sample;
    }

    /**
     * Retrieve a {@link AgreeSetSample} from the {@link #agreeSetSamples} with a
     * best possible sampling ratio among all those that comprise the given {@code focus}.
     *
     * @param focus {@link Column}s that may be focused
     * @return the {@link AgreeSetSample}
     */
    public AgreeSetSample getAgreeSetSample(Vertical focus) {
        ArrayList<Map.Entry<Vertical, AgreeSetSample>> correlationProviderEntries =
                this.agreeSetSamples.getSubsetEntries(focus);
        AgreeSetSample sample = null;
        for (Map.Entry<Vertical, AgreeSetSample> correlationProviderEntry : correlationProviderEntries) {
            AgreeSetSample nextSample = correlationProviderEntry.getValue();

            if (sample == null || nextSample.getSamplingRatio() > sample.getSamplingRatio()) {
                sample = nextSample;
            }
        }

        return sample;
    }

    /**
     * Retrieve the {@link ColumnLayoutRelationData} associated with this instance.
     *
     * @return the {@link ColumnLayoutRelationData}
     */
    public ColumnLayoutRelationData getRelationData() {
        return this.relationData;
    }

    /**
     * Retrieve the {@link RelationSchema} associated with this instance.
     *
     * @return the {@link RelationSchema}
     */
    public RelationSchema getSchema() {
        return this.relationData.getSchema();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.memoryWatchdog.stop();
    }

    /**
     * Contains data on the execution performance in a {@link SearchSpace}.
     */
    public static class ProfilingData implements Serializable {
        public final AtomicLong initializationMillis = new AtomicLong(0L);
        public final AtomicLong launchpadMillis = new AtomicLong(0L);
        public final AtomicLong ascendMillis = new AtomicLong(0L);
        public final AtomicLong trickleDownMillis = new AtomicLong(0L);
        public final AtomicLong recursionMillis = new AtomicLong(0L);
        public final AtomicLong operationMillis = new AtomicLong(0L);

        public final AtomicLong numAscends = new AtomicLong(0L);
        public final AtomicLong numTrickleDowns = new AtomicLong(0L);

        public final AtomicLong hittingSetNanos = new AtomicLong(0L);
        public final AtomicLong errorCalculationNanos = new AtomicLong(0L);
        public final AtomicLong samplingNanos = new AtomicLong(0L);
        public final AtomicLong errorEstimationNanos = new AtomicLong(0L);

        public final AtomicLong numHittingSets = new AtomicLong(0L);
        public final AtomicLong numErrorCalculations = new AtomicLong(0L);
        public final AtomicLong numSamplings = new AtomicLong(0L);
        public final AtomicLong numErrorEstimations = new AtomicLong(0L);

        public final AtomicLong numDependencies = new AtomicLong(0L);
        public final AtomicLong dependencyArity = new AtomicLong(0L);
        public final AtomicLong ascensionHeight = new AtomicLong(0L);
        public final AtomicLong trickleDepth = new AtomicLong(0L);
        public final AtomicLong mispredictions = new AtomicLong(0L);
        public final AtomicDouble errorRmse = new AtomicDouble(0d);
        public final AtomicLong errorRmseCounter = new AtomicLong(0L);
        public final AtomicLong ascendErrorCalculations = new AtomicLong(0L);
        public final AtomicLong ceilingErrorCalculations = new AtomicLong(0L);
        public final AtomicLong trickleErrorCalculations = new AtomicLong(0L);
        public final AtomicLong verifyErrorCalculations = new AtomicLong(0L);

        public final Map<SearchSpace, AtomicLong> searchSpaceMillis = Collections.synchronizedMap(new HashMap<>());

        public void printReport(String title, PrintStream out) {
            out.printf("=======================================================================================\n");
            out.printf("Report for %s\n", title);
            out.printf("---Phases------------------------------------------------------------------------------\n");
            out.printf("Initialization:                                                  %,10.3f s (%.2f%%)\n", initializationMillis.get() / 1000d, getRuntimePercentage(initializationMillis.get()));
            out.printf("Launchpads:                                                      %,10.3f s (%.2f%%)\n", launchpadMillis.get() / 1000d, getRuntimePercentage(launchpadMillis.get()));
            out.printf("Ascensions:                                                      %,10.3f s (%.2f%%)\n", ascendMillis.get() / 1000d, getRuntimePercentage(ascendMillis.get()));
            out.printf("Trickles:                                                        %,10.3f s (%.2f%%)\n", trickleDownMillis.get() / 1000d, getRuntimePercentage(trickleDownMillis.get()));
            out.printf("Recursions:                                                      %,10.3f s (%.2f%%)\n", recursionMillis.get() / 1000d, getRuntimePercentage(recursionMillis.get()));
            out.printf("Total:                                                           %,10.3f s\n", (initializationMillis.get() + operationMillis.get()) / 1000d);
            out.printf("- -Counts- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n");
            out.printf("Ascensions:                                                      %,10d #\n", numAscends.get());
            out.printf("Trickles:                                                        %,10d #\n", numTrickleDowns.get());
            out.printf("---Operations--------------------------------------------------------------------------\n");
            out.printf("Sampling:                                                        %,10.3f s (%.2f%%)\n", samplingNanos.get() / 1e9d, getRuntimePercentage(samplingNanos.get() * 1e-6));
            out.printf("Error estimation:                                                %,10.3f s (%.2f%%)\n", errorEstimationNanos.get() / 1e9d, getRuntimePercentage(errorEstimationNanos.get() * 1e-6));
            out.printf("Error calculation:                                               %,10.3f s (%.2f%%)\n", errorCalculationNanos.get() / 1e9d, getRuntimePercentage(errorCalculationNanos.get() * 1e-6));
            out.printf("Hitting sets:                                                    %,10.3f s (%.2f%%)\n", hittingSetNanos.get() / 1e9d, getRuntimePercentage(hittingSetNanos.get() * 1e-6));
            out.printf("- -Counts- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n");
            out.printf("Sampling:                                                        %,10d #\n", numSamplings.get());
            out.printf("Error estimation:                                                %,10d #\n", numErrorEstimations.get());
            out.printf("Error calculation:                                               %,10d #\n", numErrorCalculations.get());
            out.printf("Hitting sets:                                                    %,10d #\n", numHittingSets.get());
            out.printf("---Miscellaneous-----------------------------------------------------------------------\n");
            out.printf("Dependencies:                                                    %,10d #\n", numDependencies.get());
            out.printf("Arity:                                                           %,10.3f\n", dependencyArity.get() / (double) numDependencies.get());
            out.printf("Ascension height:                                                %,10.3f\n", ascensionHeight.get() / (double) numAscends.get());
            out.printf("Trickle depth:                                                   %,10.3f\n", trickleDepth.get() / (double) numTrickleDowns.get());
            out.printf("Error estimate RMSE:                                             %,10.3f\n", Math.sqrt(errorRmse.get() / errorRmseCounter.get()));
            out.printf("Mispredictions:                                                  %,10d #\n", mispredictions.get());
            out.printf("Error calculation efficiency:                                    %,10.3f ms/calculation\n", errorCalculationNanos.get() / numDependencies.doubleValue() / 1e6);
            out.printf("Ascend error calculations:                                       %,10d # (%.2f%%)\n", ascendErrorCalculations.get(), ascendErrorCalculations.get() / numErrorCalculations.doubleValue() * 100);
            out.printf("Ceiling error calculations:                                      %,10d # (%.2f%%)\n", ceilingErrorCalculations.get(), ceilingErrorCalculations.get() / numErrorCalculations.doubleValue() * 100);
            out.printf("Trickle error calculations:                                      %,10d # (%.2f%%)\n", trickleErrorCalculations.get(), trickleErrorCalculations.get() / numErrorCalculations.doubleValue() * 100);
            out.printf("Verification error calculations:                                 %,10d # (%.2f%%)\n", verifyErrorCalculations.get(), verifyErrorCalculations.get() / numErrorCalculations.doubleValue() * 100);
            out.printf("---Search spaces-----------------------------------------------------------------------\n");
            List<Map.Entry<SearchSpace, AtomicLong>> searchSpaceMillisRanking = new ArrayList<>(searchSpaceMillis.entrySet());
            searchSpaceMillisRanking.sort(Comparator.comparingLong(e -> -e.getValue().get()));
            for (Map.Entry<SearchSpace, AtomicLong> entry : searchSpaceMillisRanking) {
                String key = entry.getKey().toString();
                out.printf("%-64s %,10.3f s (%.2f%%)\n",
                        key.substring(0, Math.min(63, key.length())) + ":",
                        entry.getValue().get() / 1000d,
                        getRuntimePercentage(entry.getValue().get())
                );

            }
            out.printf("=======================================================================================\n");
        }

        private double getRuntimePercentage(double millis) {
            return 100d * millis / (initializationMillis.get() + operationMillis.get());
        }

    }
}
