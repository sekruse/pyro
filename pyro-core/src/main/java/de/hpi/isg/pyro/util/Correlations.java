package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.Arrays;

/**
 * Utilities to calculate the correlation of {@link Vertical}s.
 */
public class Correlations {

    public static double absoluteCorrelation(Vertical ab) {
        return ab.getNep();
    }

    public static double expectedCorrelation(Vertical a, Vertical b, Relation r) {
        return a.getNep() * b.getNep() / r.getNumTuplePairs();
    }

    public static double normalizedCorrelation(Vertical a, Vertical b, Vertical ab, Relation r) {
        double expectedCorrelation = expectedCorrelation(a, b, r);
        double actualCorrelation = absoluteCorrelation(ab);
        if (actualCorrelation == expectedCorrelation) {
            return 0;

        } else if (actualCorrelation < expectedCorrelation) {
            double minCorrelation = Math.max(0, a.getNep() + b.getNep() - r.getNumTuplePairs());
            return (actualCorrelation - expectedCorrelation) / (expectedCorrelation - minCorrelation);

        } else {
            double maxCorrelation = Math.min(a.getNep(), b.getNep());
            return (actualCorrelation - expectedCorrelation) / (maxCorrelation - expectedCorrelation);
        }
    }

    public static double normalizedCorrelationSignificance(Vertical a, Vertical b, Vertical ab, Relation r, boolean isNormalize) {
        double expectedCorrelation = expectedCorrelation(a, b, r);
        double actualCorrelation = absoluteCorrelation(ab);
        if (actualCorrelation == expectedCorrelation) {
            return 0;

        } else if (actualCorrelation < expectedCorrelation) {
            double minCorrelation = Math.max(0, a.getNep() + b.getNep() - r.getNumTuplePairs());
            return (expectedCorrelation - minCorrelation) / (isNormalize ? r.getNumTuplePairs() : 1d);

        } else {
            double maxCorrelation = Math.min(a.getNep(), b.getNep());
            return (maxCorrelation - expectedCorrelation) / (isNormalize ? r.getNumTuplePairs() : 1d);
        }
    }

    public static double normalizedCorrelation2(Vertical a, Vertical b, Vertical ab, Relation r) {
        double expectedCorrelation = expectedCorrelation(a, b, r);
        if (expectedCorrelation == 0) {
            return 0;
        }
        return absoluteCorrelation(ab) / expectedCorrelation - 1;
    }

    public static double absoluteConditionalCorrelation(Vertical abc) {
        return absoluteCorrelation(abc);
    }

    public static double expectedConditionalCorrelation(Vertical ac, Vertical bc, Vertical c) {
        double cNep = c.getNep();
        if (cNep == 0) return 0;
        return ac.getNep() * bc.getNep() / cNep;
    }

    private static double expectedCorrelation(double a, double b, double u) {
        if (u == 0) return 0;
        return a * b / u;
    }


    public static double normalizedEPConditionalCorrelation(Vertical ac, Vertical bc, Vertical c, Vertical abc) {
        double expectedCorrelation = expectedConditionalCorrelation(ac, bc, c);
        double actualCorrelation = absoluteConditionalCorrelation(abc);
        if (actualCorrelation == expectedCorrelation) {
            return 0;

        } else if (actualCorrelation < expectedCorrelation) {
            double minCorrelation = Math.max(0, ac.getNep() + bc.getNep() - c.getNep());
            return (actualCorrelation - expectedCorrelation) / (expectedCorrelation - minCorrelation);

        } else {
            double maxCorrelation = Math.min(ac.getNep(), bc.getNep());
            return (actualCorrelation - expectedCorrelation) / (maxCorrelation - expectedCorrelation);
        }
    }

    public static double normalizedIPConditionalCorrelation(Vertical a, Vertical b, Vertical c, Vertical ab, Vertical ac, Vertical bc, Vertical abc) {
        // At first, we need to calculate the common IPs of A+C and B+C, respectively.
        double numTuplePairs = c.getRelation().getNumTuplePairs();
        double commonNipAC = numTuplePairs - a.getNep() - c.getNep() + ac.getNep();
        double commonNipBC = numTuplePairs - b.getNep() - c.getNep() + bc.getNep();

        // Now, we can calculate their expected conditional correlation w.r.t. C.
        double expectedCorrelation = expectedCorrelation(commonNipAC, commonNipBC, c.getNip());

        // Next, we need to calculate the actual conditional correlation of A and B w.r.t. C. That is, we need to
        // find the common IPs of all three verticals. We can do this via the inclusion-exclusion principle.
        double actualCorrelation = a.getNip() + b.getNip() + c.getNip()
                - ab.getNip() - ac.getNip() - bc.getNip()
                + abc.getNip();

        // Calculate the normalized measure.
        if (actualCorrelation == expectedCorrelation) {
            return 0;

        } else if (actualCorrelation < expectedCorrelation) {
            double minCorrelation = Math.max(0, commonNipAC + commonNipBC - c.getNep());
            return (actualCorrelation - expectedCorrelation) / (expectedCorrelation - minCorrelation);

        } else {
            double maxCorrelation = Math.min(commonNipAC, commonNipBC);
            return (actualCorrelation - expectedCorrelation) / (maxCorrelation - expectedCorrelation);
        }
    }

    public static double normalizedConditionalCorrelation2(Vertical ac, Vertical bc, Vertical c, Vertical abc) {
        double expectedCorrelation = expectedConditionalCorrelation(ac, bc, c);
        if (expectedCorrelation == 0) {
            return 0;
        }
        return absoluteConditionalCorrelation(abc) / expectedCorrelation - 1;
    }

    public static double logSplittings(Vertical a) {
        PositionListIndex pli = a.getPositionListIndex();

        int numerator = pli.getRelationSize();
        IntList denominator = new IntArrayList();
        int[] clusterSizes = new int[pli.getIndex().size()];
        int i = 0;
        for (IntArrayList cluster : pli.getIndex()) {
            clusterSizes[i++] = cluster.size();
        }
        Arrays.sort(clusterSizes);
        int lastClusterSize = -1;
        int numClustersWithLastClusterSize = 0;
        for (i = 0; i < clusterSizes.length; i++) {
            int clusterSize = clusterSizes[i];
            for (int j = 2; j <= clusterSize; j++) {
                denominator.add(j);
            }
            if (clusterSize > lastClusterSize) {
                for (int j = 2; j <= numClustersWithLastClusterSize; j++) {
                    denominator.add(j);
                }
                lastClusterSize = clusterSize;
                numClustersWithLastClusterSize = 1;
            } else {
                numClustersWithLastClusterSize++;
            }
        }
        for (int j = 2; j <= numClustersWithLastClusterSize; j++) {
            denominator.add(j);
        }
        // Don't forget the singleton clusters, though.
        for (i = 2; i <= pli.getNumSingletonClusters(); i++) {
            denominator.add(i);
        }

        double logSplittings = 0;
        while (numerator > 1 || !denominator.isEmpty()) {
            if (numerator > 1 && (denominator.isEmpty() || logSplittings >= 0)) {
                logSplittings += Math.log(numerator--);
            } else {
                double v = denominator.removeInt(denominator.size() - 1);
                logSplittings -= Math.log(v);
            }
        }

        return logSplittings;
    }


}
