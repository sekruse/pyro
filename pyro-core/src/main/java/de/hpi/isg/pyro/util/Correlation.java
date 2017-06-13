package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleList;

import java.util.Comparator;

/**
 * Describes in how far two {@link Vertical}s correlate.
 */
public class Correlation {

    public static final Comparator<Correlation> fdnessComparator = Comparator.comparing(
            correlation -> correlation.getFdness() * (1 - correlation.getAtLeastProbability())
    );

    public static final Comparator<Correlation> heuristicFdnessComparator = Comparator.comparing(
            correlation -> correlation.getFdness() * correlation.getFdInterestingness()
    );

    public static final Comparator<Correlation> antiFdnessComparator = Comparator.comparing(
            correlation -> (1 - correlation.getFdness()) * (1 - correlation.getAtMostProbability())
    );

    private final Relation relation;

    private final Vertical vertical1, vertical2;

    private final double mutualInformation;

    private final double atLeastProbability, atMostProbability;

    private final double sampleMean, sampleVariance;

    public static Correlation createCorrelation(
            Relation relation,
            Vertical vertical1,
            Vertical vertical2,
            double mutualInformation,
            DoubleList randomMutualInformationSample) {

        final double epsilon = 0.0001d;
        double atLeastProbability, atMostProbability;

        // Detect whether there is a sufficiently large interval for the mutual information to be in.
        double minMutualInformation = Math.max(0, vertical1.getEntropy() + vertical2.getEntropy() - relation.getMaximumEntropy());
        double maxMutualInformation = Math.min(vertical1.getEntropy(), vertical2.getEntropy());

        // Estimate the probabilities.
        int numSmallerObservations = 0, numGreaterObservations = 0;
        for (DoubleIterator iterator = randomMutualInformationSample.iterator(); iterator.hasNext(); ) {
            double mutualInformationSpecimen = iterator.nextDouble();
            if (mutualInformationSpecimen <= mutualInformation) numSmallerObservations++;
            if (mutualInformationSpecimen >= mutualInformation) numGreaterObservations++;
        }

        atMostProbability = numSmallerObservations / (double) randomMutualInformationSample.size();
        atLeastProbability = numGreaterObservations / (double) randomMutualInformationSample.size();

        // Calculate the sample variance and mean.
        double sampleMean = 0, sampleVariance = 0;
        for (DoubleIterator iterator = randomMutualInformationSample.iterator(); iterator.hasNext(); ) {
            sampleMean += iterator.nextDouble();
        }
        sampleMean /= randomMutualInformationSample.size();
        for (DoubleIterator iterator = randomMutualInformationSample.iterator(); iterator.hasNext(); ) {
            double delta = sampleMean - iterator.nextDouble();
            sampleVariance += delta * delta;
        }
        sampleVariance /= randomMutualInformationSample.size();


        return new Correlation(
                relation,
                vertical1,
                vertical2,
                mutualInformation,
                atLeastProbability,
                atMostProbability,
                sampleMean,
                sampleVariance
        );
    }

    public Correlation(Relation relation,
                       Vertical vertical1,
                       Vertical vertical2,
                       double mutualInformation,
                       double atLeastProbability,
                       double atMostProbability,
                       double sampleMean,
                       double sampleVariance) {
        this.relation = relation;
        this.vertical1 = vertical1;
        this.vertical2 = vertical2;
        this.mutualInformation = mutualInformation;
        this.atLeastProbability = atLeastProbability;
        this.atMostProbability = atMostProbability;
        this.sampleMean = sampleMean;
        this.sampleVariance = sampleVariance;
    }

    public Relation getRelation() {
        return relation;
    }

    public Vertical getVertical1() {
        return vertical1;
    }

    public Vertical getVertical2() {
        return vertical2;
    }

    public double getMutualInformation() {
        return mutualInformation;
    }

    public double getAtLeastProbability() {
        return atLeastProbability;
    }

    public double getAtMostProbability() {
        return atMostProbability;
    }

    public double getFdInterestingness() {
        final double minEntropy = Math.min(this.vertical1.getEntropy(), this.vertical2.getEntropy());
        final double maxEntropy = Math.max(this.vertical1.getEntropy(), this.vertical2.getEntropy());

        return Math.pow(
                4 * minEntropy / this.relation.getMaximumEntropy() * (1 - maxEntropy / this.relation.getMaximumEntropy()),
                0.25
        );
    }

    public double getSampleMean() {
        return sampleMean;
    }

    public double getSampleVariance() {
        return sampleVariance;
    }

    public double getFdness() {
        final double minEntropy = Math.min(this.vertical1.getEntropy(), this.vertical2.getEntropy());
        if (minEntropy == 0d) return 1;
        return this.mutualInformation / minEntropy;
    }

    /**
     * Tells whether the given {@link Column} can yield any interesting (partial) FD when used as LHS.
     * In other words, ...?
     *
     * @param column
     * @param relation
     * @return
     */
    public static boolean isInterestingLhs(Column column, Relation relation) {
        return true;
    }

    /**
     * Tells whether the given {@link Column} can yield any interesting (partial) FD when used as LHS.
     * In other words, ...?
     *
     * @param column
     * @param relation
     * @return
     */
    public static boolean isInterestingRhs(Column column, Relation relation) {
        return true;
    }
}
