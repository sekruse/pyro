package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.core.ProfilingContext;
import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Vertical;

/**
 * Rates a partial functional dependency.
 */
@FunctionalInterface
public interface PartialFdScoring {

    /**
     * Removes some trailing digits from an error so as to avoid problems with floating point inaccurracies. This method
     * will only return {@code 0} when the input is {@code 0}.
     *
     * @param error the error to round
     * @return the rounded error
     */
    static double round(double error) {
        return Math.ceil(error * 32768d) / 32768d; // 32768 = 2^15
    }

    /**
     * Rate the a partial functional dependency.
     *
     * @param lhs              the left-hand side of the partial FD
     * @param rhs              the right-hand side of the partial FD
     * @param profilingContext the context from which the partial FD was obtained
     * @return the rating
     */
    double rate(Vertical lhs, Column rhs, ProfilingContext profilingContext);

    /**
     * Scores partial FDs by modeling the overlap of inequality pairs of the LHS and RHS as a hypergeometric
     * distribution and estimate the area under the right tail.
     */
    PartialFdScoring hypergeometricPairScoring = (lhs, rhs, profilingContext) -> {
        if (lhs.getArity() == 0) {
            return -Math.log(0.5);
        } else {
            PositionListIndex rhsPli = profilingContext.pliCache.getOrCreateFor(rhs, profilingContext);
            PositionListIndex lhsPli = profilingContext.pliCache.getOrCreateFor(lhs, profilingContext);
            PositionListIndex jointPli = profilingContext.pliCache.getOrCreateFor(lhs.union(rhs), profilingContext);
            return -HyperGeometricDistributions.estimateLogRightTailArea(
                    lhsPli.getNepAsLong(),
                    rhsPli.getNepAsLong(),
                    jointPli.getNepAsLong(),
                    profilingContext.relationData.getNumTuplePairs()
            );
        }
    };

    /**
     * Scores partial FDs by modeling the overlap of inequality pairs of the LHS and RHS as a hypergeometric
     * distribution and estimate the area under the right tail.
     */
    PartialFdScoring hypergeometricEntropyScoring = (lhs, rhs, profilingContext) -> {
        if (lhs.getArity() == 0) {
            return -Math.log(0.5);
        } else {
            PositionListIndex rhsPli = profilingContext.pliCache.getOrCreateFor(rhs, profilingContext);
            PositionListIndex lhsPli = profilingContext.pliCache.getOrCreateFor(lhs, profilingContext);
            PositionListIndex jointPli = profilingContext.pliCache.getOrCreateFor(lhs.union(rhs), profilingContext);
            double maximumEntropy = profilingContext.relationData.getMaximumEntropy();
            double mutualInformation = lhsPli.getEntropy() + rhsPli.getEntropy() - jointPli.getEntropy();
            int scalingFactor = profilingContext.getRelationData().getNumRows();
            double score = -HyperGeometricDistributions.estimateLogRightTailArea(
                    (long) (scalingFactor * lhsPli.getEntropy() / maximumEntropy),
                    (long) (scalingFactor * rhsPli.getEntropy() / maximumEntropy),
                    (long) (scalingFactor * mutualInformation / maximumEntropy),
                    scalingFactor
            );
            return score;
        }
    };

    /**
     * Scores partial FDs by modeling the overlap of inequality pairs of the LHS and RHS as a hypergeometric
     * distribution and estimate the area under the right tail.
     */
    PartialFdScoring hypergeometricSimpleScoring = (lhs, rhs, profilingContext) -> {
        if (lhs.getArity() == 0) {
            return 0;
        } else {
            PositionListIndex rhsPli = profilingContext.pliCache.getOrCreateFor(rhs, profilingContext);
            PositionListIndex lhsPli = profilingContext.pliCache.getOrCreateFor(lhs, profilingContext);
            PositionListIndex jointPli = profilingContext.pliCache.getOrCreateFor(lhs.union(rhs), profilingContext);
            double expectedJointNeps = HyperGeometricDistributions.mean(
                    rhsPli.getNepAsLong(), lhsPli.getNepAsLong(), profilingContext.relationData.getNumTuplePairs()
            );
            double stddev = HyperGeometricDistributions.stddev(
                    rhsPli.getNepAsLong(), lhsPli.getNepAsLong(), profilingContext.relationData.getNumTuplePairs()
            );
            return (jointPli.getNep() - expectedJointNeps) / stddev;
        }
    };

    /**
     * Scores partial FDs via the conditional probability
     * <i>P(t[RHS] = t'[RHS] | t[LHS] = t'[RHS])</i>.
     */
    PartialFdScoring conditionalG1Scoring = (lhs, rhs, profilingContext) -> {
        if (lhs.getArity() == 0) {
            return 0;
        } else {
            PositionListIndex lhsPli = profilingContext.pliCache.getOrCreateFor(lhs, profilingContext);
            double lhsNep = lhsPli.getNep();
            if (lhsNep == 0) return 1;

            PositionListIndex jointPli = profilingContext.pliCache.getOrCreateFor(lhs.union(rhs), profilingContext);
            ;
            return jointPli.getNep() / lhsNep;
        }
    };

    /**
     * Scores partial FDs as the product of {@link #hypergeometricSimpleScoring} and {@link #conditionalG1Scoring}.
     */
    PartialFdScoring pyroScoring = (lhs, rhs, profilingContext) -> {
        double simpleScore = hypergeometricSimpleScoring.rate(lhs, rhs, profilingContext);
        double conditionalG1 = conditionalG1Scoring.rate(lhs, rhs, profilingContext);
        return Math.signum(simpleScore) * conditionalG1 * Math.log(simpleScore);
    };

}
