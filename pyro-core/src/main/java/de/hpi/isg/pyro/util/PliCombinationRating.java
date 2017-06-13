package de.hpi.isg.pyro.util;

/**
 * Rates a partial functional dependency.
 */
@FunctionalInterface
public interface PliCombinationRating {

    /**
     * Rate the combination of two {@link PositionListIndex}es.
     *
     * @return the rating
     */
    double rate(PositionListIndex x, PositionListIndex a, PositionListIndex xa);

    static PliCombinationRating createConstantRater(double rating) {
        return (x, a, xa) -> rating;
    }

    PliCombinationRating logHyperGeometricRightTailbound = (x, a, xa) ->
            HyperGeometricDistributions.estimateLogRightTailArea(
                    Math.round(x.getNep()),
                    Math.round(a.getNep()),
                    Math.round(xa.getNep()),
                    Math.round(x.getMaximumNip())
            );

    PliCombinationRating partialLogHyperGeometricRightTailbound = (x, a, xa) -> {
        if (x.getNumNulls() == 0) return logHyperGeometricRightTailbound.rate(x, a, xa);

        return HyperGeometricDistributions.estimateLogRightTailArea(
                Math.round(x.getNepWithout(x.getNullCluster())),
                Math.round(a.getNepWithout(x.getNullCluster())),
                Math.round(xa.getNepWithout(x.getNullCluster())),
                Math.round(x.getMaximumNipWithout(x.getNullCluster()))
        );
    };

    PliCombinationRating logHyperGeometricLeftTailbound = (x, a, xa) ->
            HyperGeometricDistributions.estimateLogLeftTailArea(
                    Math.round(x.getNep()),
                    Math.round(a.getNep()),
                    Math.round(xa.getNep()),
                    Math.round(x.getMaximumNip())
            );

    PliCombinationRating partialLogHyperGeometricLeftTailbound = (x, a, xa) -> {
        if (x.getNumNulls() == 0) return logHyperGeometricLeftTailbound.rate(x, a, xa);

        return HyperGeometricDistributions.estimateLogLeftTailArea(
                Math.round(x.getNepWithout(x.getNullCluster())),
                Math.round(a.getNepWithout(x.getNullCluster())),
                Math.round(xa.getNepWithout(x.getNullCluster())),
                Math.round(x.getMaximumNipWithout(x.getNullCluster()))
        );
    };

    /**
     * This instance expresses the probability for two tuples that agree on {@code x} to disagree on {@code a}.
     */
    PliCombinationRating conflictIpRatio = (x, a, xa) ->
            (xa.getNip() - x.getNip()) / x.getNep();

    /**
     * This instance expresses the probability for two tuples that agree on {@code x} to disagree on {@code a}, where
     * {@code x} might have ignored {@code null}s.
     */
    PliCombinationRating partialConflictIpRatio = (x, a, xa) -> x.getNumNulls() == 0 ?
            conflictIpRatio.rate(x, a, xa) :
            (xa.getNipWithout(x.getNullCluster()) - x.getNipWithout(x.getNullCluster())) / x.getNepWithout(x.getNullCluster());


}
