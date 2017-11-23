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
    PartialFdScoring hypergeometricScoring = (lhs, rhs, profilingContext) -> {
        PositionListIndex rhsPli = profilingContext.pliCache.getOrCreateFor(rhs, profilingContext);
        if (lhs.getArity() == 0) {
            return Math.log(0.5);
        } else {
            PositionListIndex lhsPli = profilingContext.pliCache.getOrCreateFor(lhs, profilingContext);
            PositionListIndex jointPli = profilingContext.pliCache.getOrCreateFor(lhs.union(rhs), profilingContext);
            return HyperGeometricDistributions.estimateLogRightTailArea(
                    lhsPli.getNepAsLong(),
                    rhsPli.getNepAsLong(),
                    jointPli.getNepAsLong(),
                    profilingContext.relationData.getNumTuplePairs()
            );
        }
    };

}
