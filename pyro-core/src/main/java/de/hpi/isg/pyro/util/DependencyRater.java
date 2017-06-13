package de.hpi.isg.pyro.util;

/**
 * Rates a dependency based on its number of (in)equality pairs.
 */
@FunctionalInterface
public interface DependencyRater {

    /**
     * Rate some dependency based on some {@link de.hpi.isg.pyro.model.Vertical} {@code X} and some
     * {@link de.hpi.isg.pyro.model.Column} {@code A}.
     *
     * @param nepX   number of equality pairs of {@code X}
     * @param nepA   number of equality pairs of {@code A}
     * @param nepXA  number of equality pairs of {@code XA}
     * @param maxNap overall number of pairs
     * @return the score
     */
    double rate(double nepX, double nepA, double nepXA, double maxNap);

    static double nepToNip(double nep, double maxNap) {
        return maxNap - nep;
    }

    static double nipToNep(double nip, double maxNap) {
        return maxNap - nip;
    }

    static double nipXA(double nepX, double nepA, double nepXA, double maxNap) {
        return maxNap - (nepX + nepA - nepXA);
    }

    /**
     * Measures a dependency {@code (X, A)} by considering {@code X}'s IPs are distributed w.r.t. {@code A}'s IPs.
     * That is, -1 means anti-correlation, 0 means independence, and 1 means total correlation of {@code X} and {@code A}.
     */
    DependencyRater nipBalance = (nepX, nepA, nepXA, maxNap) -> {
        // If we have a no room for permutations (X and A are keys or constant), then we define the score to be 0.
        if (nepX == 0 || nepX == maxNap || nepA == 0 || nepA == maxNap) return 0d;

        double nipXA = nipXA(nepX, nepA, nepXA, maxNap);

        // Percentage of tuples that disagree on A, that also disagree on X.
        double nipXWhereNipA = nipXA / nepToNip(nepA, maxNap);

        // Percentage of tuples that agree on A, that disagree on X
        double nipXWhereNepA = (nepToNip(nepX, maxNap) - nipXA) / nepA;

        // If X and A are correlating, X's IPs should correlate with A's IPs.
        // Other way around, if X and A are anti-correlating, X's IPs should correlate with A's IPs.
        return nipXWhereNipA - nipXWhereNepA;
    };

    DependencyRater logHypergeometricRightTailbound = (nepX, nepA, nepXA, maxNap) ->
            -HyperGeometricDistributions.estimateLogRightTailArea(
                    (long) nepX,
                    (long) nepA,
                    (long) nepXA,
                    (long) maxNap
            );

    DependencyRater logHypergeometricLeftTailbound = (nepX, nepA, nepXA, maxNap) ->
            -HyperGeometricDistributions.estimateLogLeftTailArea(
                    (long) nepX,
                    (long) nepA,
                    (long) nepXA,
                    (long) maxNap
            );

}
