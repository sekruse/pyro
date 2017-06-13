package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.ColumnLayoutRelation;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.RowLayoutRelation;
import de.hpi.isg.pyro.model.Vertical;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.BitSet;
import java.util.Map;
import java.util.Random;

/**
 * This class uses sampling of tuple pairs to provide correlation estimates of {@link Vertical}s.
 */
public class TrieAgreeSetSample extends AgreeSetSample {

    private static final Factory<TrieAgreeSetSample> factory = TrieAgreeSetSample::new;

    /**
     * Stores the sample as a mapping from agree sets to their (absolute) frequency.
     */
    public final VerticalMap<Long> correlations;

    /**
     * Create a new instance.
     *
     * @param relation       for that the correlation estimates should be provided
     * @param samplingFactor is defined as {@code |sampled tuple pairs|/|tuples in relation|}
     * @param random         an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    public static TrieAgreeSetSample createFor(ColumnLayoutRelation relation, double samplingFactor, Random random) {
        return createFor(relation, (int) (relation.getNumTuplePairs() * samplingFactor), random, factory);
    }

    /**
     * Create a new instance.
     *
     * @param relation   for that the correlation estimates should be provided
     * @param sampleSize the number of tuple pairs to sample
     * @param random     an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    public static TrieAgreeSetSample createFor(ColumnLayoutRelation relation, int sampleSize, Random random) {
        return createFor(relation, sampleSize, random, factory);
    }

    /**
     * Create a new, focused instance. That is, tuple pairs will be drawn from a restricted area only.
     *
     * @param relation            for that the correlation estimates should be provided
     * @param restrictionVertical only tuple pairs that agree on this {@link Vertical} should be considered
     * @param restrictionPli      helps to enforce the restriction efficiently
     * @param samplingFactor      is defined as {@code |sampled tuple pairs|/|tuples in relation|}
     * @param random              an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    public static TrieAgreeSetSample createFocusedFor(ColumnLayoutRelation relation,
                                                      Vertical restrictionVertical,
                                                      PositionListIndex restrictionPli,
                                                      double samplingFactor,
                                                      Random random) {
        return createFocusedFor(
                relation, restrictionVertical, restrictionPli, (int) (samplingFactor * restrictionPli.getNep()), random, factory
        );
    }

    /**
     * Create a new, focused instance. That is, tuple pairs will be drawn from a restricted area only.
     *
     * @param relation            for that the correlation estimates should be provided
     * @param restrictionVertical only tuple pairs that agree on this {@link Vertical} should be considered
     * @param restrictionPli      helps to enforce the restriction efficiently
     * @param sampleSize          the number of tuple pairs to sample
     * @param random              an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    public static TrieAgreeSetSample createFocusedFor(ColumnLayoutRelation relation,
                                                      Vertical restrictionVertical,
                                                      PositionListIndex restrictionPli,
                                                      int sampleSize,
                                                      Random random) {

        return createFocusedFor(relation, restrictionVertical, restrictionPli, sampleSize, random, factory);
    }

    /**
     * Create a new instance.
     *
     * @param relation       for that the correlation estimates should be provided
     * @param samplingFactor is defined as {@code |sampled tuple pairs|/|tuples in relation|}
     * @param random         an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    public static TrieAgreeSetSample createFor(RowLayoutRelation relation, double samplingFactor, Random random) {
        return createFor(relation, samplingFactor, random, factory);
    }

    private TrieAgreeSetSample(Relation relation, Vertical focus, int sampleSize, long populationSize,
                               Object2LongOpenHashMap<BitSet> agreeSetCounters) {
        super(relation, focus, sampleSize, populationSize);
        ObjectIterator<Object2LongMap.Entry<BitSet>> counterIterator = agreeSetCounters.object2LongEntrySet().fastIterator();
        this.correlations = new VerticalMap<>(this.relation);
        while (counterIterator.hasNext()) {
            Object2LongMap.Entry<BitSet> entry = counterIterator.next();
            this.correlations.put(
                    relation.getVertical(entry.getKey()),
                    entry.getLongValue()
            );
        }
    }


    @Override
    public long getNumAgreeSupersets(Vertical agreement) {
        long _startNanos = System.nanoTime();
        long count = 0L;
        for (Map.Entry<Vertical, Long> entry : this.correlations.getSupersetEntries(agreement)) {
            count += entry.getValue();
        }
        _nanoQueries.addAndGet(System.nanoTime() - _startNanos);
        _numQueries.incrementAndGet();
        return count;
    }

    @Override
    public long getNumAgreeSupersets(Vertical agreement, Vertical disagreement) {
        long _startNanos = System.nanoTime();
        long count = 0L;
        for (Map.Entry<Vertical, Long> entry : this.correlations.getRestrictedSupersetEntries(agreement, disagreement)) {
            count += entry.getValue();
        }
        _nanoQueries.addAndGet(System.nanoTime() - _startNanos);
        _numQueries.incrementAndGet();
        return count;
    }

    public double getFocusSelectivity() {
        return populationSize / (double) relation.getNumTuplePairs();
    }

    public double getSamplingRatio() {
        return this.sampleSize / (double) this.populationSize;
    }

    public Vertical getFocus() {
        return this.focus;
    }

    public boolean isExact() {
        return this.populationSize == this.sampleSize;
    }

    public long getPopulationSize() {
        return this.populationSize;
    }

    @Override
    public String toString() {
        return String.format("%s on %s (size=%,d, ratio=%.03f)",
                this.getClass().getSimpleName(), this.focus, this.sampleSize, this.getSamplingRatio()
        );
    }
}
