package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnLayoutRelation;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.*;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.*;

/**
 * This class contains the relevant data to operate Pyro within a JVM.
 */
public class ProfilingContext {

    /**
     * The configuration of Pyro.
     */
    final Configuration configuration;

    /**
     * Caches {@link PositionListIndex}es.
     */
    PLICache pliCache;

    /**
     * Caches {@link AgreeSetSample}s.
     */
    VerticalMap<Reference<AgreeSetSample>> agreeSetSamples;

    /**
     * Defines {@link AgreeSetSample}s that should not be cleared.
     */
    final Collection<AgreeSetSample> stickyAgreeSetSamples = new LinkedList<>();

    /**
     * The {@link ColumnLayoutRelation} to be profiled.
     */
    private ColumnLayoutRelation relation;

    /**
     * Provides randomness.
     */
    Random random;

    /**
     * Creates a new instance.
     *
     * @param configuration the configuration for Pyro
     */
    public ProfilingContext(Configuration configuration) {
        this.configuration = configuration;
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
                this.relation,
                focus,
                this.pliCache.getPositionListIndex(focus),
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
     * Retrieve the {@link ColumnLayoutRelation} associated with this instance.
     *
     * @return the {@link ColumnLayoutRelation}
     */
    public ColumnLayoutRelation getRelation() {
        return this.relation;
    }

}
