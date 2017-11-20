package de.hpi.isg.pyro.core;

import com.google.common.util.concurrent.AtomicDouble;
import de.hpi.isg.pyro.model.*;
import de.hpi.isg.pyro.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.Serializable;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * This class contains the relevant data to operate Pyro within a JVM.
 */
public class ProfilingContext extends DependencyConsumer {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The configuration of Pyro.
     */
    final Configuration configuration;

    /**
     * Caches {@link PositionListIndex}es.
     */
    final PLICache pliCache;

    /**
     * Caches {@link AgreeSetSample}s.
     */
    final VerticalMap<Reference<AgreeSetSample>> agreeSetSamples;

    /**
     * Defines {@link AgreeSetSample}s that should not be cleared.
     */
    final Collection<AgreeSetSample> stickyAgreeSetSamples = new LinkedList<>();

    /**
     * The {@link ColumnLayoutRelationData} to be profiled.
     */
    final ColumnLayoutRelationData relationData;

    /**
     * Provides randomness.
     */
    final Random random;

    /**
     * Collects performance profiling data.
     */
    public final ProfilingData profilingData = new ProfilingData();

    /**
     * Creates a new instance.
     *
     * @param configuration the configuration for Pyro
     * @param relationData  that should be profiled
     */
    public ProfilingContext(Configuration configuration,
                            ColumnLayoutRelationData relationData,
                            Consumer<PartialKey> uccConsumer,
                            Consumer<PartialFD> fdConsumer) {
        final long startMillis = System.currentTimeMillis();
        this.configuration = configuration;
        this.relationData = relationData;
        this.uccConsumer = uccConsumer;
        this.fdConsumer = fdConsumer;
        this.random = this.configuration.seed == null ? new Random() : new Random(this.configuration.seed);
        this.pliCache = new PLICache(
                relationData,
                configuration.parallelism != 1,
                configuration.isUseWeakReferencesForPlis ? WeakReference::new : SoftReference::new
        );

        if (configuration.sampleSize > 0) {
            // Create the initial samples.
            RelationSchema schema = this.relationData.getSchema();
            this.agreeSetSamples = configuration.parallelism > 1 ?
                    new SynchronizedVerticalMap<>(schema) :
                    new VerticalMap<>(schema);

            // Make sure to always have a cover of correlation providers (i.e., make the GC always spare the initial providers).
            for (Column column : schema.getColumns()) {
                // TODO: Create samples in parallel.
                AgreeSetSample sample = this.createFocusedSample(column, 1d, false);
                synchronized (this.stickyAgreeSetSamples) {
                    this.stickyAgreeSetSamples.add(sample);
                }
            }
        } else {
            this.agreeSetSamples = null;
        }

        this.profilingData.initializationMillis.addAndGet(System.currentTimeMillis() - startMillis);
    }

    /**
     * Create and cache an {@link AgreeSetSample}.
     *
     * @param focus       the sampling focus
     * @param boostFactor the sampling boost (cf. {@link Configuration#sampleSize} and {@link Configuration#sampleBooster})
     * @return the created {@link AgreeSetSample}
     */
    AgreeSetSample createFocusedSample(Vertical focus, double boostFactor) {
        return this.createFocusedSample(focus, boostFactor, this.configuration.isUseWeakReferencesForSamples);
    }

    /**
     * Create and cache an {@link AgreeSetSample}.
     *
     * @param focus              the sampling focus
     * @param boostFactor        the sampling boost (cf. {@link Configuration#sampleSize} and {@link Configuration#sampleBooster})
     * @param isUseWeakReference whether to use a {@link WeakReference} rather than a {@link SoftReference} to cache
     *                           the new {@link AgreeSetSample}
     * @return the created {@link AgreeSetSample}
     */
    AgreeSetSample createFocusedSample(Vertical focus, double boostFactor, boolean isUseWeakReference) {
        final long startNanos = System.nanoTime();
        ListAgreeSetSample sample = ListAgreeSetSample.createFocusedFor(
                this.relationData,
                focus,
                this.pliCache.getOrCreateFor(focus),
                (int) (this.configuration.sampleSize * boostFactor),
                this.random
        );
        if (logger.isDebugEnabled()) logger.debug("Created {} with a boost factor of {}.", sample, boostFactor);
        this.agreeSetSamples.put(focus, isUseWeakReference ? new WeakReference<>(sample) : new SoftReference<>(sample));
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
        ArrayList<Map.Entry<Vertical, Reference<AgreeSetSample>>> correlationProviderEntries =
                this.agreeSetSamples.getSubsetEntries(focus);
        AgreeSetSample sample = null;
        for (Map.Entry<Vertical, Reference<AgreeSetSample>> correlationProviderEntry : correlationProviderEntries) {
            AgreeSetSample nextSample = correlationProviderEntry.getValue().get();

            if (sample == null || nextSample != null && nextSample.getSamplingRatio() > sample.getSamplingRatio()) {
                sample = nextSample;
            }
        }

        // It seems that the JVM might discard our SoftReferences even though we made the referencee strongly referenced.
        if (sample == null) {
            System.out.printf(
                    "Warning: Could not find any sample for %s. Sticky samples: %s. Found entries: %s.",
                    focus, this.stickyAgreeSetSamples, correlationProviderEntries
            );
            for (AgreeSetSample nextSample : this.stickyAgreeSetSamples) {
                if (!focus.contains(nextSample.getFocus())) continue;

                if (sample == null || nextSample.getSamplingRatio() > sample.getSamplingRatio()) {
                    sample = nextSample;
                }
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
