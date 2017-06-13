package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.List;

/**
 * Rates a partial functional dependency.
 */
@FunctionalInterface
public interface PFDRater {

    /**
     * Rate the partial FD {@code X -> A}.
     *
     * @param x  the left-hand side of the PFD
     * @param a  the right-hand side of the PFD
     * @param xa the combination of {@code x} and {@code a}
     * @param r  the {@link Relation} containing {@code x} and {@code a}
     * @return the rating
     */
    double rate(Vertical x, Column a, Vertical xa, Relation r);

    static PFDRater createConstantRater(double rating) {
        return (x, a, xa, r) -> rating;
    }

    PFDRater logHyperGeometricConfidenceGT = (x, a, xa, r) ->
            HyperGeometricDistributions.estimateLogRightTailArea(
                    Math.round(x.getNep()),
                    Math.round(a.getNep()),
                    Math.round(xa.getNep()),
                    Math.round(r.getMaximumNip())
            );

    PFDRater upperLogHyperGeometricConfidenceGT = (x, a, xa, r) ->
            HyperGeometricDistributions.estimateLogRightTailArea(
                    Math.round(x.getNep()),
                    Math.round(a.getNep()),
                    Math.round(xa.getNep()),
                    Math.round(r.getMaximumNip())
            );

    PFDRater scaledLogHyperGeometricConfidenceGT = (x, a, xa, r) -> {
        double sf1 = x.getPositionListIndex().getNumNonRedundantEP() / x.getNep();
        double sf2 = a.getPositionListIndex().getNumNonRedundantEP() / a.getNep();
        double sf = Math.max(sf1, sf2);
        sf = xa.getPositionListIndex().getNumNonRedundantEP() / xa.getNep();

        return HyperGeometricDistributions.estimateLogRightTailArea(
                Math.round(x.getNep() * sf),
                Math.round(a.getNep() * sf),
                Math.round(xa.getNep() * sf),
                Math.round(r.getMaximumNip() * sf)
        );
    };

    PFDRater logHyperGeometricConfidenceLT = (x, a, xa, r) ->
            HyperGeometricDistributions.estimateLogLeftTailArea(
                    Math.round(x.getNep()),
                    Math.round(a.getNep()),
                    Math.round(xa.getNep()),
                    Math.round(r.getMaximumNip())
            );

    PFDRater upperLogHyperGeometricConfidenceLT = (x, a, xa, r) ->
            HyperGeometricDistributions.estimateLogLeftTailArea(
                    Math.round(x.getNep()),
                    Math.round(a.getNep()),
                    Math.round(xa.getNep()),
                    Math.round(r.getMaximumNip())
            );

    PFDRater scaledLogHyperGeometricConfidenceLT = (x, a, xa, r) -> {
        double sf1 = x.getPositionListIndex().getNumNonRedundantEP() / x.getNep();
        double sf2 = a.getPositionListIndex().getNumNonRedundantEP() / a.getNep();
        double sf = Math.max(sf1, sf2);
        sf = xa.getPositionListIndex().getNumNonRedundantEP() / xa.getNep();

        return HyperGeometricDistributions.estimateLogLeftTailArea(
                Math.round(x.getNep() * sf),
                Math.round(a.getNep() * sf),
                Math.round(xa.getNep() * sf),
                Math.round(r.getMaximumNip() * sf)
        );
    };


    PFDRater rightTailAreaFDRater = (x, a, xa, r) ->
            HyperGeometricDistributions.estimateRightTailArea(
                    Math.round(x.getEntropy() * r.getNumRows()),
                    Math.round(a.getEntropy() * r.getNumRows()),
                    Math.round((x.getEntropy() + a.getEntropy() - xa.getEntropy()) * r.getNumRows()),
                    Math.round(r.getMaximumEntropy() * r.getNumRows())
            );

    PFDRater rightTailAreaNipFDRater = (x, a, xa, r) ->
            HyperGeometricDistributions.estimateRightTailArea2(
                    Math.round(x.getNip()),
                    Math.round(a.getNip()),
                    Math.round(x.getNip() + a.getNip() - xa.getNip()),
                    Math.round(r.getMaximumNip())
            );

    PFDRater leftTailAreaNipFDRater = (x, a, xa, r) ->
            HyperGeometricDistributions.estimateLeftTailArea(
                    Math.round(x.getNip()),
                    Math.round(a.getNip()),
                    Math.round(x.getNip() + a.getNip() - xa.getNip()),
                    Math.round(r.getMaximumNip())
            );


    PFDRater rightTailPositionFDRater = (x, a, xa, r) ->
            Math.max(0, HyperGeometricDistributions.calculateRightTailPosition(
                    Math.round(x.getEntropy() * r.getNumRows()),
                    Math.round(a.getEntropy() * r.getNumRows()),
                    Math.round((x.getEntropy() + a.getEntropy() - xa.getEntropy()) * r.getNumRows()),
                    Math.round(r.getMaximumEntropy() * r.getNumRows())
            ));

    PFDRater relativeEntropyCoverFDRater = (x, a, xa, r) ->
            a.getEntropy() == 0 ?
                    0 :
                    (a.getEntropy() - (x.getEntropy() + a.getEntropy() - xa.getEntropy())) / a.getEntropy();

    PFDRater nipPmiFDRater = (x, a, xa, r) -> {
        if (x.getNip() == r.getMaximumNip() || a.getNip() == r.getMaximumNip()) return 0d;
        if (xa.getNip() == r.getMaximumNip()) return Double.POSITIVE_INFINITY;

        double hX = Math.log(r.getMaximumNip()) - Math.log(r.getMaximumNip() - x.getNip());
        double hA = Math.log(r.getMaximumNip()) - Math.log(r.getMaximumNip() - a.getNip());
        double hXA = Math.log(r.getMaximumNip()) - Math.log(r.getMaximumNip() - xa.getNip());

        return (hX + hA - hXA);
    };

    PFDRater nipNpmiFDRater = (x, a, xa, r) -> {
        if (x.getNip() == r.getMaximumNip() || a.getNip() == r.getMaximumNip()) return 0d;
        if (xa.getNip() == r.getMaximumNip()) return 1;

        double hX = Math.log(r.getMaximumNip()) - Math.log(r.getMaximumNip() - x.getNip());
        double hA = Math.log(r.getMaximumNip()) - Math.log(r.getMaximumNip() - a.getNip());
        double hXA = Math.log(r.getMaximumNip()) - Math.log(r.getMaximumNip() - xa.getNip());

        return (hX + hA - hXA) / hXA;
    };

    /**
     * Given an FD candidate {@code X->A}, rate its error as via the conditional entropy {@code H(A|X)}.
     */
    PFDRater conditionalEntropy = (x, a, xa, r) -> xa.getEntropy() - x.getEntropy();

    /**
     * Given an FD candidate {@code X->A}, rate its error as via the average conditional entropy {@code H(A|X=x)}.
     */
    PFDRater clusterwiseConditionalEntropy = (x, a, xa, r) -> {
        List<PositionListIndex.ClusterMapping> mapping = x.getPositionListIndex().mapClustersWith(xa.getPositionListIndex());
        double conditionalInfo = 0d;
        for (PositionListIndex.ClusterMapping clusterMapping : mapping) {
            IntArrayList xCluster = clusterMapping.getOriginalCluster();
            double clusterConditionalInfo = xCluster.size() * Math.log(xCluster.size());
            for (IntArrayList xaCluster : clusterMapping.getRefinedClusters()) {
                clusterConditionalInfo -= xaCluster.size() * Math.log(xaCluster.size());
            }
            conditionalInfo += clusterConditionalInfo / Math.log(2);
        }
        return conditionalInfo / r.getNumRows();
    };

    PFDRater nipKeyErrorRater = (x, a, xa, r) -> x.getNep() / r.getMaximumNip();

    PFDRater nipToDVKeyErrorRater = (x, a, xa, r) ->
            1 - (r.getNumRows() / (2 * x.getNep() + r.getNumRows()));

    PFDRater nepPerTupleKeyErrorRater = (x, a, xa, r) ->
            InequalityPairs.epPerTuple(x.getNip(), r.getMaximumNip());

    PFDRater nepPerTuplePartialKeyFDErrorRater = (x, a, xa, r) -> {
        // Partial relation that comprises tuple pairs only where there is a disagreement on a.
        double partialMaxNip = a.getNip();
        // Hence, we can only retain IPs w.r.t. x that are also IPs w.r.t. to a.
        double partialXNip = x.getNip() + a.getNip() - xa.getNip();

        return InequalityPairs.epPerTuple(partialXNip, partialMaxNip);
    };

    PFDRater absoluteEntropyCoverFDRater = (x, a, xa, r) ->
            (a.getEntropy() - (x.getEntropy() + a.getEntropy() - xa.getEntropy())) / r.getMaximumEntropy();

    PFDRater dvRateFDErrorRater = (x, a, xa, r) -> {
        double xDVs = InequalityPairs.nipToDistinctValues(x.getNip(), r.getNumRows());
        double xaDVs = InequalityPairs.nipToDistinctValues(xa.getNip(), r.getNumRows());
        return (xaDVs - xDVs) / xDVs;
    };

    /**
     * This instance expresses the probability for two tuples that agree on {@code x} to disagree on {@code a}.
     */
    PFDRater conflictIpRatioFDErrorRater = (x, a, xa, r) -> {
        double xNep = x.getNep();
        return xNep == 0 ? 0 : (xNep - xa.getNep()) / xNep;
    };

    /**
     * This instance expresses the probability for two tuples that disagree on {@code a} to agree on {@code x}.
     */
    PFDRater conflictEpRatioFDErrorRater = (x, a, xa, r) ->
            (x.getNep() - xa.getNep()) / a.getNip();

    /**
     * This instance expresses the probability that a randomly sampled tuple pair is a FD violation.
     */
    PFDRater conflictRatioFDErrorRater = (x, a, xa, r) ->
            (x.getNep() - xa.getNep()) / r.getMaximumNip();

    PFDRater nipPerTupleFDErrorRater = (x, a, xa, r) ->
            InequalityPairs.ipPerTuple(xa.getNip() - x.getNip(), x.getNep());

    PFDRater nipPartialKeynessFDRater = (x, a, xa, r) ->
            InequalityPairs.keyness(xa.getNip() - x.getNip(), x.getNep());

    PFDRater entropyGradientFDRater = (x, a, xa, r) -> {
        double error = absoluteEntropyCoverFDRater.rate(x, a, xa, r);
        double xConstancy = x.getEntropy() / r.getMaximumEntropy();
        double aKeyness = (r.getMaximumEntropy() - a.getEntropy()) / r.getMaximumEntropy();
        if (xConstancy + aKeyness > 1) {
            double delta = xConstancy + aKeyness - 1;
            xConstancy -= delta / 2;
            aKeyness -= delta / 2;
        }
        return 4 * (1 - error) * xConstancy * aKeyness;
    };

    PFDRater nipGradientFdRater = (x, a, xa, r) -> {
        double partialKeyness = InequalityPairs.keyness(xa.getNip() - x.getNip(), x.getNep());

        return partialKeyness;

//        double error = conflictIpRatioFDErrorRater.rate(x, a, xa, r);
//        double xConstancy = Math.sqrt(x.getNep()) / Math.sqrt(r.getMaximumNip());
//        double aKeyness = Math.sqrt(a.getNip()) / Math.sqrt(r.getMaximumNip());
//        if (xConstancy + aKeyness > 1) {
//            double delta = xConstancy + aKeyness - 1;
//            xConstancy -= delta / 2;
//            aKeyness -= delta / 2;
//        }
//        return 4 * (1 - error) * xConstancy * aKeyness;
    };

    // Measures from literature.

    /**
     * For a FD {@code X -> A}, calculate the probability {@code P(t[A] = t'[A] | t[X] = t'[X]} (where {@code t} and
     * {@code t'} can be equal and appear in any order).
     */
    PFDRater pdep = (x, a, xa, r) -> {
        List<PositionListIndex.ClusterMapping> clusterRefinement =
                x.getPositionListIndex().mapClustersWith(xa.getPositionListIndex());
        double pdep = x.getPositionListIndex().getNumSingletonClusters();
        for (PositionListIndex.ClusterMapping clusterMapping : clusterRefinement) {
            double partialPdep = clusterMapping.getNumRefinedSingletonClusters();
            for (IntArrayList refinedCluster : clusterMapping.getRefinedClusters()) {
                partialPdep += refinedCluster.size() * refinedCluster.size();
            }
            partialPdep /= clusterMapping.getOriginalCluster().size();
            pdep += partialPdep;
        }

        pdep /= r.getNumRows();
        return pdep;
    };

    /**
     * For a vertical {@code X}, calculate the probability {@code P(t[X] = t'[X]} (where {@code t} and
     * {@code t'} can be equal and appear in any order).
     */
    PFDRater unaryPdep = (x, a, xa, r) -> {
        PositionListIndex xPli = x.getPositionListIndex();
        double pdep = xPli.getNumSingletonClusters();
        for (IntArrayList cluster : xPli.getIndex()) {
            pdep += cluster.size() * cluster.size();
        }
        pdep /= r.getNumRows();
        pdep /= r.getNumRows();
        return pdep;
    };

    /**
     * For a FD {@code X -> A}, calculate the absolute increase in probability between
     * {@code P(t[A] = t'[A] | t[X] = t'[X]} and {@code P(t[A] = t'[A]} (where {@code t} and
     * {@code t'} can be equal and appear in any order).
     */
    PFDRater pdepLift = (x, a, xa, r) -> pdep.rate(x, a, xa, r) - unaryPdep.rate(a, null, null, r);

    /**
     * For a FD {@code X -> A}, calculate the absolute increase in probability between
     * {@code P(t[A] = t'[A] | t[X] = t'[X]} and {@code P(t[A] = t'[A]} normalized to the maximum possible
     * increase (where {@code t} and
     * {@code t'} can be equal and appear in any order).
     */
    PFDRater tau = (x, a, xa, r) -> {
        double pdep = PFDRater.pdep.rate(x, a, xa, r);
        double unaryPdep = PFDRater.unaryPdep.rate(a, null, null, r);
        return (pdep - unaryPdep) / (1 - unaryPdep);
    };

    /**
     * A version of {@link #pdep} that is normalized w.r.t. its expectation value.
     */
    PFDRater mu = (x, a, xa, r) -> {
        double pdep = PFDRater.pdep.rate(x, a, xa, r);
        double unaryPdep = PFDRater.unaryPdep.rate(a, null, null, r);
        return 1 - (1 - pdep) / (1 - unaryPdep)
                * (x.getPositionListIndex().getNumClusters() - 1)
                / (r.getNumRows() - 1);
    };

    /**
     * Determine the relevance of an FD as the number of all tuple pairs (ordered, with equality) that agree on
     * the antecedent.
     */
    PFDRater fdAsArSupport = (x, a, xa, r) -> {
        PositionListIndex xaPli = xa.getPositionListIndex();
        double accu = xaPli.getNumClusters() - xaPli.getNumNonSingletonClusters();
        for (IntArrayList cluster : xaPli.getIndex()) {
            accu += cluster.size() * cluster.size();
        }
        return accu / r.getNumRows() / r.getNumRows();
    };


    /**
     * Determine the quality of an FD as the number of all tuple pairs (ordered, with equality) that agree on
     * the antecedent and consequent divided by the number of tuple pairs agreeing on the antecedent.
     */
    PFDRater fdAsArConfidence = (x, a, xa, r) -> {
        PositionListIndex xPli = x.getPositionListIndex();
        double numXAgreements = xPli.getNumClusters() - xPli.getNumNonSingletonClusters();
        for (IntArrayList cluster : xPli.getIndex()) {
            numXAgreements += cluster.size() * cluster.size();
        }
        PositionListIndex xaPli = xa.getPositionListIndex();
        double numXAAgreements = xaPli.getNumClusters() - xaPli.getNumNonSingletonClusters();
        for (IntArrayList cluster : xaPli.getIndex()) {
            numXAAgreements += cluster.size() * cluster.size();
        }
        return numXAAgreements / numXAgreements;
    };

    /**
     * Determine the quality and relevance of an FD by how much the knowledge of {@code t[X] = t'[X]} lifts the
     * probability that {@code t[A] = t'[A]}.
     */
    PFDRater fdAsArCertaintyFactor = (x, a, xa, r) -> {
        PositionListIndex aPli = a.getPositionListIndex();
        double aSupport = aPli.getNumClusters() - aPli.getNumNonSingletonClusters();
        for (IntArrayList cluster : aPli.getIndex()) {
            aSupport += cluster.size() * cluster.size();
        }
        aSupport /= r.getNumRows();
        aSupport /= r.getNumRows();
        double confidence = fdAsArConfidence.rate(x, a, xa, r);

        return confidence > aSupport ?
                (confidence - aSupport) / (1 - aSupport) :
                (confidence - aSupport) / aSupport;
    };

    /**
     * Rates either {@code 0} if there is an FD or {@code 1} otherwise.
     */
    PFDRater binaryRater = (x, a, xa, r) -> x.getPositionListIndex().size() == xa.getPositionListIndex().size() ? 0 : 1;

    /**
     * The G1 measure of Kivinen and Mannila: The number of tuple pairs that violate an FD.
     */
    PFDRater G1 = (x, a, xa, r) -> {
        PositionListIndex xPli = x.getPositionListIndex();
        double xAgreements = xPli.getNumSingletonClusters();
        for (IntArrayList cluster : xPli.getIndex()) {
            xAgreements += cluster.size() * (double) cluster.size();
        }
        PositionListIndex xaPli = xa.getPositionListIndex();
        double xaAgreements = xaPli.getNumSingletonClusters();
        for (IntArrayList cluster : xaPli.getIndex()) {
            xaAgreements += cluster.size() * (double) cluster.size();
        }

        return xAgreements - xaAgreements;
    };

    /**
     * The g1 measure of Kivinen and Mannila: The fraction of tuple pairs that violate an FD.
     */
    PFDRater g1 = (x, a, xa, r) -> G1.rate(x, a, xa, r) / r.getNumRows() / r.getNumRows();

    /**
     * The g1 measure of Kivinen and Mannila: The fraction of tuple pairs that violate an FD (modified version).
     */
    PFDRater g1prime = (x, a, xa, r) -> round((xa.getNip() - x.getNip()) / r.getNumTuplePairs());

    /**
     * The G2 measure of Kivinen and Mannila: The number of tuples for that there is a further tuple that, together
     * with the former tuple, violates and FD.
     */
    PFDRater G2 = (x, a, xa, r) -> {
        // We calculate this measure as follows:
        // First, we map the clusters in X to clusters in XA.
        PositionListIndex xPli = x.getPositionListIndex();
        PositionListIndex xaPli = xa.getPositionListIndex();
        List<PositionListIndex.ClusterMapping> clusterMappings = xPli.mapClustersWith(xaPli);

        // Then, we simply count the number of tuples in actually refined clusters.
        double count = 0d;
        for (PositionListIndex.ClusterMapping clusterMapping : clusterMappings) {
            if (clusterMapping.getNumRefinedSingletonClusters() > 0 || clusterMapping.getRefinedClusters().size() > 1) {
                count += clusterMapping.getOriginalCluster().size();
            }
        }

        return count;
    };

    /**
     * The g2 measure of Kivinen and Mannila: The fraction of tuples for that there is a further tuple that, together
     * with the former tuple, violates and FD.
     */
    PFDRater g2 = (x, a, xa, r) -> G2.rate(x, a, xa, r) / r.getNumRows();

    /**
     * The G3 measure of Kivinen and Mannila: The minimum number of tuples to remove from the relation so that
     * an FD is not violated.
     */
    PFDRater G3 = (x, a, xa, r) -> {
        // We calculate this measure different from the standard Tane procedure:
        // First, we map the clusters in X to clusters in XA.
        PositionListIndex xPli = x.getPositionListIndex();
        PositionListIndex xaPli = xa.getPositionListIndex();
        List<PositionListIndex.ClusterMapping> clusterMappings = xPli.mapClustersWith(xaPli);

        // Then, we simply count the items in the largest cluster.
        double numRetainedTuples = xPli.getNumSingletonClusters();
        for (PositionListIndex.ClusterMapping clusterMapping : clusterMappings) {
            int largestClusterSize = 1;
            for (IntArrayList cluster : clusterMapping.getRefinedClusters()) {
                if (cluster.size() > largestClusterSize) largestClusterSize = cluster.size();
            }
            numRetainedTuples += largestClusterSize;
        }

        return r.getNumRows() - numRetainedTuples;
    };

    /**
     * The g3 measure of Kivinen and Mannila: The minimum fraction of tuples to remove from the relation so that
     * an FD is not violated.
     */
    PFDRater g3 = (x, a, xa, r) -> G3.rate(x, a, xa, r) / r.getNumRows();

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

}
