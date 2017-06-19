package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.*;
import de.hpi.isg.pyro.util.*;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.function.Consumer;

/**
 * This class contains the relevant data to operate Pyro within a JVM.
 */
public class ProfilingContext extends DependencyConsumer {

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
     * Creates a new instance.
     *
     * @param configuration the configuration for Pyro
     * @param relationData  that should be profiled
     */
    public ProfilingContext(Configuration configuration,
                            ColumnLayoutRelationData relationData,
                            Consumer<PartialKey> uccConsumer,
                            Consumer<PartialFD> fdConsumer) {
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
        ListAgreeSetSample sample = ListAgreeSetSample.createFocusedFor(
                this.relationData,
                focus,
                this.pliCache.getOrCreateFor(focus),
                (int) (this.configuration.sampleSize * boostFactor),
                this.random
        );
        this.info("Created %s with a boost factor of %,f\n", sample, boostFactor);
        this.agreeSetSamples.put(focus, isUseWeakReference ? new WeakReference<>(sample) : new SoftReference<>(sample));
        return sample;
    }


    final void info(String msg, Object... params) {
        if (this.configuration.isVerbose) {
            System.out.printf(msg, params);
        }
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

}
