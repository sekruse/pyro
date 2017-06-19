package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.commons.math3.distribution.NormalDistribution;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class uses sampling of tuple pairs to provide correlation estimates of {@link Vertical}s.
 */
public abstract class AgreeSetSample {

    // Profiling data.
    public static final AtomicLong _numSamples = new AtomicLong(0);
    public static final AtomicLong _milliSampling = new AtomicLong(0);
    public static final AtomicLong _numQueries = new AtomicLong(0);
    public static final AtomicLong _nanoQueries = new AtomicLong(0);

    /**
     * Factory for new instances.
     */
    public interface Factory<T extends AgreeSetSample> {

        T create(RelationData relationData, Vertical focus, int sampleSize, long populationSize,
                 Object2LongOpenHashMap<BitSet> agreeSetCounter);

    }

    /**
     * The {@link RelationData} that has been sampled.
     */
    protected final RelationData relationData;

    /**
     * If this instance describes a focused sampling, i.e., it is based on tuple pairs that agree on some
     * {@link Column}s, then these {@link Column}s are captured as the sampling {@link #focus}.
     */
    protected final Vertical focus;

    /**
     * The number of sampled tuple pairs.
     */
    public final int sampleSize;

    /**
     * The size of the population from that the sample has been taken. This is governed by the {@link #focus}.
     */
    protected final long populationSize;

    /**
     * When estimating the standard deviation of a sampling query, this is the number of counter examples to add
     * to smooth out very small sample query results..
     */
    private static final double stdDevSmoothing = 1;

    /**
     * Create a new instance.
     *
     * @param relationData   for that the correlation estimates should be provided
     * @param sampleSize the number of tuple pairs to sample
     * @param random     an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    protected static <T extends AgreeSetSample> T createFor(ColumnLayoutRelationData relationData, int sampleSize, Random random,
                                                            Factory<T> factory) {
        long startTime = System.currentTimeMillis();

        if (random == null) random = new Random();

        // First, do the sampling and count the detected agree sets.
        Object2LongOpenHashMap<BitSet> agreeSetCounters = new Object2LongOpenHashMap<>(relationData.getNumRows());
        agreeSetCounters.defaultReturnValue(0L);
        System.out.printf("Sampling %d tuple pairs to provide correlation estimates... ", sampleSize);
        sampleSize = (int) Math.min(sampleSize, relationData.getNumTuplePairs());

        for (long i = 0; i < sampleSize; i++) {
            // Sample two tuples.
            int tupleIndex1 = random.nextInt(relationData.getNumRows());
            int tupleIndex2 = random.nextInt(relationData.getNumRows());
            if (tupleIndex1 == tupleIndex2) {
                i--;
                continue;
            }

            // Create the agree set.
            BitSet agreeSet = new BitSet(relationData.getNumColumns());
            for (ColumnData columnData : relationData.getColumnData()) {
                int value1 = columnData.getProbingTableValue(tupleIndex1);
                if (value1 != PositionListIndex.singletonValueId && value1 == columnData.getProbingTableValue(tupleIndex2)) {
                    agreeSet.set(columnData.getColumn().getIndex());
                }
            }

            // Count the agree set.
            agreeSetCounters.addTo(agreeSet, 1);
        }

        // Now, put the counts into a VerticalMap, so as to enable subset and superset queries.
        T instance = factory.create(
                relationData, relationData.getSchema().emptyVertical, sampleSize, relationData.getNumTuplePairs(), agreeSetCounters
        );

        long endTime = System.currentTimeMillis();
        _milliSampling.addAndGet(endTime - startTime);
        _numSamples.incrementAndGet();
        return instance;
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
    protected static <T extends AgreeSetSample> T createFocusedFor(ColumnLayoutRelationData relation,
                                                                   Vertical restrictionVertical,
                                                                   PositionListIndex restrictionPli,
                                                                   double samplingFactor,
                                                                   Random random,
                                                                   Factory<T> factory) {
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
    protected static <T extends AgreeSetSample> T createFocusedFor(ColumnLayoutRelationData relation,
                                                                   Vertical restrictionVertical,
                                                                   PositionListIndex restrictionPli,
                                                                   int sampleSize,
                                                                   Random random,
                                                                   Factory<T> factory) {
        long _startMillis = System.currentTimeMillis();

        if (random == null) random = new Random();

        // Initialize some helper data structures.
        BitSet freeColumnIndices = new BitSet(relation.getNumColumns());
        freeColumnIndices.set(0, relation.getNumColumns());
        freeColumnIndices.andNot(restrictionVertical.getColumnIndices());
        ColumnData[] relevantColumnData = new ColumnData[freeColumnIndices.cardinality()];
        for (int columnIndex = freeColumnIndices.nextSetBit(0), i = 0;
             columnIndex != -1;
             columnIndex = freeColumnIndices.nextSetBit(columnIndex + 1), i++) {
            relevantColumnData[i] = relation.getColumnData(i);
        }
        BitSet agreeSetPrototype = (BitSet) restrictionVertical.getColumnIndices().clone();
        Object2LongOpenHashMap<BitSet> agreeSetCounters = new Object2LongOpenHashMap<>(relation.getNumRows());
        agreeSetCounters.defaultReturnValue(0);

        // Analyzing the PLI as to what we can sample.
        double restrictionNep = restrictionPli.getNep();
        sampleSize = (int) Math.min(sampleSize, restrictionNep);
        if (sampleSize >= restrictionNep) {
            for (IntArrayList cluster : restrictionPli.getIndex()) {
                // Calculate how much to sample from the current cluster.
                for (int i = 0; i < cluster.size(); i++) {
                    int tupleIndex1 = cluster.getInt(i);

                    for (int j = i + 1; j < cluster.size(); j++) {
                        int tupleIndex2 = cluster.getInt(j);

                        // Create the agree set.
                        BitSet agreeSet = (BitSet) agreeSetPrototype.clone();
                        for (ColumnData columnData : relevantColumnData) {
                            int value1 = columnData.getProbingTableValue(tupleIndex1);
                            if (value1 != PositionListIndex.singletonValueId && value1 == columnData.getProbingTableValue(tupleIndex2)) {
                                agreeSet.set(columnData.getColumn().getIndex());
                            }
                        }

                        // Count the agree set.
                        agreeSetCounters.addTo(agreeSet, 1);
                    }
                }
            }

        } else {
            // Index the clusters by their probability of providing a random tuple pair.
            double[] clusterProbabilityIndex = new double[restrictionPli.getNumNonSingletonClusters() - 1];
            long numSeenTuplePairs = 0L;
            for (int i = 0; i < clusterProbabilityIndex.length; i++) {
                IntArrayList cluster = restrictionPli.getIndex().get(i);
                long numTuplePairs = cluster.size() * (cluster.size() - 1L) / 2L;
                numSeenTuplePairs += numTuplePairs;
                clusterProbabilityIndex[i] = numSeenTuplePairs / restrictionNep;
            }

            // Now do the actual sampling.
            for (int i = 0; i < sampleSize; i++) {
                // Sample a cluster to draw the tuple pair from.
                int clusterIndex = Arrays.binarySearch(clusterProbabilityIndex, random.nextDouble());
                if (clusterIndex < 0) clusterIndex = -(clusterIndex + 1);
                IntArrayList cluster = restrictionPli.getIndex().get(clusterIndex);

                // Get two distinct tuples from the cluster.
                int tupleIndex1 = random.nextInt(cluster.size());
                int tupleIndex2;
                do {
                    tupleIndex2 = random.nextInt(cluster.size());
                } while (tupleIndex1 == tupleIndex2);
                tupleIndex1 = cluster.getInt(tupleIndex1);
                tupleIndex2 = cluster.getInt(tupleIndex2);

                // Create the agree set.
                BitSet agreeSet = (BitSet) agreeSetPrototype.clone();
                for (ColumnData columnData : relevantColumnData) {
                    int value1 = columnData.getProbingTableValue(tupleIndex1);
                    if (value1 != PositionListIndex.singletonValueId && value1 == columnData.getProbingTableValue(tupleIndex2)) {
                        agreeSet.set(columnData.getColumn().getIndex());
                    }
                }

                // Count the agree set.
                agreeSetCounters.addTo(agreeSet, 1);
            }
        }

        T sample = factory.create(
                relation, restrictionVertical, sampleSize, (long) restrictionNep, agreeSetCounters
        );
        _milliSampling.addAndGet(System.currentTimeMillis() - _startMillis);
        _numSamples.incrementAndGet();
        return sample;
    }

    /**
     * Create a new instance.
     *
     * @param relation       for that the correlation estimates should be provided
     * @param samplingFactor is defined as {@code |sampled tuple pairs|/|tuples in relation|}
     * @param random         an existing {@link Random} to introduce entropy or {@code null}
     * @return the new instance
     */
    public static <T extends AgreeSetSample> T createFor(ColumnLayoutRelationData relation, double samplingFactor, Random random,
                                                         Factory<T> factory) {
        long startTime = System.currentTimeMillis();

        if (random == null) random = new Random();

        // First, do the sampling and count the detected agree sets.
        Object2LongOpenHashMap<BitSet> agreeSetCounters = new Object2LongOpenHashMap<>(relation.getNumRows());
        agreeSetCounters.defaultReturnValue(0);
        double sampleSize = Math.round(samplingFactor * relation.getNumRows());
        System.out.printf("Sampling %,.0f tuple pairs to provide correlation estimates... ", sampleSize);

        for (long i = 0; i < sampleSize; i++) {
            // Sample two tuples.
            int tupleIndex1 = random.nextInt(relation.getNumRows());
            int tupleIndex2 = random.nextInt(relation.getNumRows());
            if (tupleIndex1 == tupleIndex2) {
                i--;
                continue;
            }

            int[] tuple1 = relation.getTuple(tupleIndex1);
            int[] tuple2 = relation.getTuple(tupleIndex2);

            // Create the agree set.
            BitSet agreeSet = new BitSet(relation.getNumColumns());
            for (int columnIndex = 0; columnIndex < relation.getNumColumns(); columnIndex++) {
                int value1 = tuple1[columnIndex];
                if (value1 != RelationData.singletonValueId && value1 == tuple2[columnIndex]) {
                    agreeSet.set(columnIndex);
                }
            }

            // Count the agree set.
            agreeSetCounters.addTo(agreeSet, 1);
        }

        // Now, put the counts into a VerticalMap, so as to enable subset and superset queries.
        T instance = factory.create(
                relation, relation.getSchema().emptyVertical, (int) sampleSize, relation.getNumTuplePairs(), agreeSetCounters
        );

        _milliSampling.addAndGet(System.currentTimeMillis() - startTime);
        _numSamples.incrementAndGet();
        return instance;
    }

    abstract public long getNumAgreeSupersets(Vertical agreement);

    /**
     * Counts agree sets.
     *
     * @param agreement    in those columns, the counted agree sets agree
     * @param disagreement in those columns, the counted agree sets disagree
     * @return the number of agree sets agreeing as requested
     */
    abstract public long getNumAgreeSupersets(Vertical agreement, Vertical disagreement);

    /**
     * Counts agree sets.
     *
     * @param agreement    in those columns, the counted agree sets agree
     * @param disagreement in those columns, the counted agree sets disagree
     * @return the number of agree sets agreeing and the number of agree sets agreeing and disagreeing as requested
     */
    public long[] getNumAgreeSupersetsExt(Vertical agreement, Vertical disagreement) {
        return new long[]{
                this.getNumAgreeSupersets(agreement),
                this.getNumAgreeSupersets(agreement, disagreement)
        };
    }

    /**
     * Estimates the fraction of tuple pairs that agree in {@code agreement} in the sampled {@link RelationSchema}.
     *
     * @param agreement the {@link Vertical} with agreeing values in the tuple pairs;
     *                  must include the {@link #getFocus() focus}
     * @return the estimate
     */
    public double estimateAgreements(Vertical agreement) {
        assert agreement.contains(this.focus);

        if (this.populationSize == 0) return 0;

        return this.obseverationsToRelationRatio(this.getNumAgreeSupersets(agreement));
    }

    /**
     * Estimates the fraction of tuple pairs that agree in {@code agreement} in the sampled {@link RelationSchema}.
     *
     * @param agreement  the {@link Vertical} with agreeing values in the tuple pairs
     * @param confidence a confidence value
     * @return the estimate as a confidence {@link ConfidenceInterval}
     */
    public ConfidenceInterval estimateAgreements(Vertical agreement, double confidence) {
        assert agreement.contains(this.focus);

        if (this.populationSize == 0) return new ConfidenceInterval(0, 0);

        // Count the sampled tuples agreeing as requested.
        long numHits = this.getNumAgreeSupersets(agreement);
        double sampleRatio = numHits / (double) this.sampleSize;
        if (this.isExact()) return new ConfidenceInterval(
                this.ratioToRelationRatio(sampleRatio), this.ratioToRelationRatio(sampleRatio)
        );

        // Calculate a confidence interval with smoothing (one counter-example) and z-score.
        NormalDistribution normalDistribution = new NormalDistribution();
        double z = normalDistribution.inverseCumulativeProbability((confidence + 1) / 2);
        double smoothedRatioPositiveTuples = (numHits + stdDevSmoothing / 2) / (this.sampleSize + stdDevSmoothing);
        double stddevPositiveTuples = Math.sqrt(smoothedRatioPositiveTuples * (1 - smoothedRatioPositiveTuples) / this.sampleSize);
        double minRatio = Math.max(sampleRatio - z * stddevPositiveTuples, calculateNonNegativeFraction(numHits, this.relationData.getNumTuplePairs()));
        double maxRatio = sampleRatio + z * stddevPositiveTuples;

        return new ConfidenceInterval(this.ratioToRelationRatio(minRatio), this.ratioToRelationRatio(maxRatio));
    }

    /**
     * Estimates the fraction of tuple pairs that agree in {@code agreement} and disagree in {@code disagreement} in the
     * sampled {@link RelationSchema}.
     *
     * @param agreement the {@link Vertical} with agreeing values in the tuple pairs;
     *                  must include the {@link #getFocus() focus}
     * @return the estimate
     */
    public double estimateMixed(Vertical agreement, Vertical disagreement) {
        assert agreement.contains(this.focus);

        if (this.populationSize == 0) return 0;

        return this.obseverationsToRelationRatio(this.getNumAgreeSupersets(agreement, disagreement));
    }

    /**
     * Estimates the fraction of tuple pairs that agree in {@code agreement} and disagree in {@code disagreement} in the
     * sampled {@link RelationSchema}.
     *
     * @param agreement  the {@link Vertical} with agreeing values in the tuple pairs;
     *                   must include the {@link #getFocus() focus}
     * @param confidence a confidence value
     * @return the estimate as a confidence {@link ConfidenceInterval}
     */
    public ConfidenceInterval estimateMixed(Vertical agreement, Vertical disagreement, double confidence) {
        assert agreement.contains(this.focus);

        if (this.populationSize == 0) return new ConfidenceInterval(0, 0);

        // Count the sampled tuples agreeing as requested.
        long numHits = this.getNumAgreeSupersets(agreement, disagreement);
        double sampleRatio = numHits / (double) this.sampleSize;

        if (this.isExact()) return new ConfidenceInterval(
                this.ratioToRelationRatio(sampleRatio), this.ratioToRelationRatio(sampleRatio)
        );

        // Calculate a confidence interval with smoothing (one counter-example) and z-score.
        NormalDistribution normalDistribution = new NormalDistribution();
        double z = normalDistribution.inverseCumulativeProbability((confidence + 1) / 2);
        double smoothedSampleRatio = (numHits + stdDevSmoothing / 2) / (this.sampleSize + stdDevSmoothing);
        double stddevPositiveTuples = Math.sqrt(smoothedSampleRatio * (1 - smoothedSampleRatio) / this.sampleSize);
        double minRatio = Math.max(sampleRatio - z * stddevPositiveTuples, calculateNonNegativeFraction(numHits, this.relationData.getNumTuplePairs()));
        double maxRatio = sampleRatio + z * stddevPositiveTuples;

        return new ConfidenceInterval(
                this.ratioToRelationRatio(minRatio),
                this.ratioToRelationRatio(maxRatio)
        );
    }

    /**
     * Estimates the fraction of tuple pairs agreeing in {@code agreement} that disagree in {@code disagreement}.
     *
     * @param agreement    the {@link Vertical} with agreeing values in the tuple pairs;
     *                     must include the {@link #getFocus() focus}
     * @param disagreement the {@link Vertical} with disagreeing values in the tuple pairs
     * @return the estimate
     */
    public double estimateConditionalDisagreements(Vertical agreement, Vertical disagreement) {
        assert agreement.contains(this.focus);

        if (this.populationSize == 0) return 0;

        long[] counts = this.getNumAgreeSupersetsExt(agreement, disagreement);

        long subsampleSize = counts[0];
        long numMatches = counts[1];
        return (subsampleSize == 0d) ? 0d : (numMatches / (double) subsampleSize);
    }

    /**
     * Estimates the fraction of tuple pairs agreeing in {@code agreement} that disagree in {@code disagreement}.
     * <p>Note the special behavior when we have no tuples that fulfill the agreement. Then we define the fraction of
     * tuples as {@code 0} (but a smoothed confidence intervals might apply.)</p>
     *
     * @param agreement    the {@link Vertical} with agreeing values in the tuple pairs;
     *                     must include the {@link #getFocus() focus}
     * @param disagreement the {@link Vertical} with disagreeing values in the tuple pairs
     * @param confidence   a confidence value
     * @return the estimate as a confidence {@link ConfidenceInterval}
     */
    public ConfidenceInterval estimateConditionalDisagreements(Vertical agreement, Vertical disagreement, double confidence) {
        assert agreement.contains(this.focus);

        if (this.populationSize == 0) return new ConfidenceInterval(0, 0);

        // Approach 1: Hierarchical sampling.
        // Count the sampled agree sets matching the agreement.
        long[] counts = this.getNumAgreeSupersetsExt(agreement, disagreement);
        long subsampleSize = counts[0];
        long numMatches = counts[1];
        double sampleRatio = (subsampleSize == 0d) ? 0d : (numMatches / (double) subsampleSize);

        // For exact samples, immediately return the value.
        if (this.isExact()) {
            return new ConfidenceInterval(sampleRatio, sampleRatio);
        }

        // Count the sampled agree sets agreeing and disagreeing as requested.
        double smoothedSampleRatio = (numMatches + stdDevSmoothing / 2) / (subsampleSize + stdDevSmoothing);

        // Calculate the standard deviation of our sample ratio.
        double stddev = Math.sqrt(smoothedSampleRatio * (1 - smoothedSampleRatio) / subsampleSize);

        // Calculate the z score for our confidence level.
        NormalDistribution normalDistribution = new NormalDistribution();
        double z = normalDistribution.inverseCumulativeProbability((confidence + 1) / 2);

        // Create our confidence interval.
        return new ConfidenceInterval(
                Math.min(sampleRatio - z * stddev, calculateNonNegativeFraction(numMatches, this.populationSize)),
                sampleRatio + z * stddev
        );
    }

    protected AgreeSetSample(RelationData relationData, Vertical focus, int sampleSize, long populationSize) {
        this.relationData = relationData;
        this.focus = focus;
        this.sampleSize = sampleSize;
        this.populationSize = populationSize;
    }

    /**
     * Extrapolate a number of observations within the sample to the expected ratio of observations within the
     * whole {@link #relationData}.
     *
     * @param numObservations the number of observations
     * @return the global ratio
     */
    private double obseverationsToRelationRatio(double numObservations) {
        return this.ratioToRelationRatio(numObservations / (double) this.sampleSize);
    }

    /**
     * Extrapolate a sample or focus proportion to the expected ratio of observations within the
     * whole {@link #relationData}.
     *
     * @param ratio a sample or focus proportion
     * @return the global ratio
     */
    private double ratioToRelationRatio(double ratio) {
        return ratio * this.populationSize / this.relationData.getNumTuplePairs();
    }

    private static double calculateNonNegativeFraction(double a, double b) {
        if (a == 0) return 0d;
        return Math.max(Double.MIN_VALUE, a / b);
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
