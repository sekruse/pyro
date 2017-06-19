package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.ColumnLayoutRelationData;
import de.hpi.isg.pyro.model.RelationData;
import de.hpi.isg.pyro.model.Vertical;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Random;

/**
 * This class uses sampling of tuple pairs to provide correlation estimates of {@link Vertical}s.
 */
public class ListAgreeSetSample extends AgreeSetSample {

    private static final Factory<ListAgreeSetSample> factory = ListAgreeSetSample::new;

    private static class Entry {

        Entry(long[] agreeSet, long count) {
            this.agreeSet = agreeSet;
            this.count = count;
        }

        final long[] agreeSet;

        final long count;
    }

    /**
     * Stores all agree sets and their counts.
     */
    public final ArrayList<Entry> agreeSetCounters = new ArrayList<>();

    /**
     * Create a new instance.
     *
     * @param relation       for that the correlation estimates should be provided
     * @param samplingFactor is defined as {@code |sampled tuple pairs|/|tuples in relation|}
     * @param random         an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    public static ListAgreeSetSample createFor(ColumnLayoutRelationData relation, double samplingFactor, Random random) {
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
    public static ListAgreeSetSample createFor(ColumnLayoutRelationData relation, int sampleSize, Random random) {
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
    public static ListAgreeSetSample createFocusedFor(ColumnLayoutRelationData relation,
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
    public static ListAgreeSetSample createFocusedFor(ColumnLayoutRelationData relation,
                                                      Vertical restrictionVertical,
                                                      PositionListIndex restrictionPli,
                                                      int sampleSize,
                                                      Random random) {

        return createFocusedFor(relation, restrictionVertical, restrictionPli, sampleSize, random, factory);
    }

//    /**
//     * Create a new instance.
//     *
//     * @param relation       for that the correlation estimates should be provided
//     * @param samplingFactor is defined as {@code |sampled tuple pairs|/|tuples in relation|}
//     * @param random         an existing {@link Random} to introduce entropy or {@code null}
//     * @return the new instance
//     */
//    public static ListAgreeSetSample createFor(RowLayoutRelation relation, double samplingFactor, Random random) {
//        return createFor(relation, samplingFactor, random, factory);
//    }

    private ListAgreeSetSample(RelationData relation, Vertical focus, int sampleSize, long populationSize,
                               Object2LongOpenHashMap<BitSet> agreeSetCounters) {
        super(relation, focus, sampleSize, populationSize);
        ObjectIterator<Object2LongMap.Entry<BitSet>> counterIterator = agreeSetCounters.object2LongEntrySet().fastIterator();
        while (counterIterator.hasNext()) {
            Object2LongMap.Entry<BitSet> entry = counterIterator.next();
            this.agreeSetCounters.add(new Entry(entry.getKey().toLongArray(), entry.getLongValue()));
        }
    }


    @Override
    public long getNumAgreeSupersets(Vertical agreement) {
        long _startNanos = System.nanoTime();
        long count = 0L;
        long[] minAgreeSet = agreement.getColumnIndices().toLongArray();
        Entries:
        for (Entry agreeSetCounter : this.agreeSetCounters) {
            long[] agreeSet = agreeSetCounter.agreeSet;
            int i = 0;
            int minFields = Math.min(agreeSet.length, minAgreeSet.length);
            while (i < minFields) {
                if ((agreeSet[i] & minAgreeSet[i]) != minAgreeSet[i]) continue Entries;
                i++;
            }
            while (i < minAgreeSet.length) {
                if (minAgreeSet[i] != 0L) continue Entries;
                i++;
            }
            count += agreeSetCounter.count;
        }
        _nanoQueries.addAndGet(System.nanoTime() - _startNanos);
        _numQueries.incrementAndGet();
        return count;
    }

    @Override
    public long getNumAgreeSupersets(Vertical agreement, Vertical disagreement) {
        long _startNanos = System.nanoTime();
        long count = 0L;
        long[] minAgreeSet = agreement.getColumnIndices().toLongArray();
        long[] minDisagreeSet = disagreement.getColumnIndices().toLongArray();
        Entries:
        for (Entry agreeSetCounter : this.agreeSetCounters) {
            long[] agreeSet = agreeSetCounter.agreeSet;
            // Check the agreement.
            int i = 0;
            int minFields = Math.min(agreeSet.length, minAgreeSet.length);
            while (i < minFields) {
                if ((agreeSet[i] & minAgreeSet[i]) != minAgreeSet[i]) continue Entries;
                i++;
            }
            while (i < minAgreeSet.length) {
                if (minAgreeSet[i] != 0L) continue Entries;
                i++;
            }
            // Check the disagreement.
            i = 0;
            minFields = Math.min(agreeSet.length, minDisagreeSet.length);
            while (i < minFields) {
                if ((agreeSet[i] & minDisagreeSet[i]) != 0L) continue Entries;
                i++;
            }

            count += agreeSetCounter.count;
        }
        _nanoQueries.addAndGet(System.nanoTime() - _startNanos);
        _numQueries.incrementAndGet();
        return count;
    }

    @Override
    public long[] getNumAgreeSupersetsExt(Vertical agreement, Vertical disagreement) {
        long _startNanos = System.nanoTime();
        long countAgreements = 0L, count = 0L;
        long[] minAgreeSet = agreement.getColumnIndices().toLongArray();
        long[] minDisagreeSet = disagreement.getColumnIndices().toLongArray();
        Entries:
        for (Entry agreeSetCounter : this.agreeSetCounters) {
            long[] agreeSet = agreeSetCounter.agreeSet;
            // Check the agreement.
            int i = 0;
            int minFields = Math.min(agreeSet.length, minAgreeSet.length);
            while (i < minFields) {
                if ((agreeSet[i] & minAgreeSet[i]) != minAgreeSet[i]) continue Entries;
                i++;
            }
            while (i < minAgreeSet.length) {
                if (minAgreeSet[i] != 0L) continue Entries;
                i++;
            }
            countAgreements += agreeSetCounter.count;

            // Check the disagreement.
            i = 0;
            minFields = Math.min(agreeSet.length, minDisagreeSet.length);
            while (i < minFields) {
                if ((agreeSet[i] & minDisagreeSet[i]) != 0L) continue Entries;
                i++;
            }

            count += agreeSetCounter.count;
        }
        _nanoQueries.addAndGet(System.nanoTime() - _startNanos);
        _numQueries.incrementAndGet();
        return new long[]{countAgreements, count};
    }

    public double getFocusSelectivity() {
        return populationSize / (double) relationData.getNumTuplePairs();
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
