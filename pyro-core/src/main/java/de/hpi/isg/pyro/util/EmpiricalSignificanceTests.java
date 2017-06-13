package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Vertical;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.Random;

/**
 * This utility helps to empirically determine the significance of a {@link Vertical} overlap.
 */
public class EmpiricalSignificanceTests {

    public static PFDRater createNipRightTailAreaRater(int repetitions, boolean isVerbose) {
        return (x, a, xa, r) -> {
            double probGreaterMutualNip = 0d;
            double mutualInfo = x.getEntropy() + a.getEntropy() - xa.getEntropy();
            double mutualNip = x.getNip() + a.getNip() - xa.getNip();

            // Materialize one vertical.
            PositionListIndex pli;
            int[] probingTable;
            if (x.getPositionListIndex().size() < a.getPositionListIndex().size()) {
                pli = x.getPositionListIndex();
                probingTable = a.getPositionListIndex().getProbingTable();
            } else {
                pli = a.getPositionListIndex();
                probingTable = x.getPositionListIndex().getProbingTable();
            }

            double hX = Math.log(r.getMaximumNip() / x.getNep());
            double hA = Math.log(r.getMaximumNip() / a.getNep());

            // Shuffle and collect the data.
            DoubleArrayList npmis = new DoubleArrayList(repetitions);
            DoubleArrayList pmis = new DoubleArrayList(repetitions);
            DoubleArrayList nips = new DoubleArrayList(repetitions);
            double meanMI = 0d;
            double meanMN = 0d;
            for (int i = 0; i < repetitions; i++) {
                Column.shuffle(probingTable, new Random());
                PositionListIndex intersection = pli.probe(probingTable);
                double randomMutualInfo = a.getEntropy() + x.getEntropy() - intersection.getEntropy();
                double randomMutualNip = a.getNip() + x.getNip() - intersection.getNip();
                meanMI += randomMutualInfo;
                meanMN += randomMutualNip;

                // Calculate the pointwise mutual information of the two columns.
                {
                    double hXA = Math.log(r.getMaximumNip() / (r.getMaximumNip() - intersection.getNip()));
                    double pmi = hX + hA - hXA;
                    pmis.add(pmi);
                    npmis.add(pmi / hXA);
                }

                if (randomMutualNip >= mutualNip) {
                    probGreaterMutualNip += 1;
                }
                nips.add(randomMutualNip);
            }
            probGreaterMutualNip /= repetitions;

            meanMN /= repetitions;
            nips.sort(Double::compareTo);
            double meanNip = 0d, varianceNip = 0d;
            for (Double nip : nips) {
                meanNip += nip;
            }
            meanNip /= nips.size();
            for (Double nip : nips) {
                varianceNip += (meanNip - nip) * (meanNip - nip);
            }
            varianceNip /= nips.size();
            if (isVerbose) {
                System.out.printf("Mean NIP: N(X)=%,.0f, N(A)=%,.0f, N(R)=%,.0f, " +
                                "N(X;A)=%,.0f, expected: %,.0f, random: %,.0f (+- %,.1f) (%s)\n",
                        x.getNip(), a.getNip(), r.getMaximumNip(),
                        mutualNip, x.getNip() * a.getNip() / r.getMaximumNip(), meanNip, Math.sqrt(varianceNip),
                        meanNip > mutualNip ? "overrated" : (meanNip < mutualNip ? "underrated" : "hit")

                );
            }


            meanMI /= repetitions;
//            System.out.printf("Mean MI:  H(X)=%.4f, H(A)=%.4f, H(R)=%.4f, I(X;A)=%.4f, expected: %.4f, observed: %.4f\n",
//                    x.getEntropy(), a.getEntropy(), r.getMaximumEntropy(), meanMI,
//                    x.getEntropy() * a.getEntropy() / r.getMaximumEntropy(), mutualInfo
//            );
            npmis.sort(Double::compareTo);
            pmis.sort(Double::compareTo);
            double minPmi = hA + hX + Math.log(Math.max(0, a.getNep() + x.getNep() - r.getMaximumNip()) / r.getMaximumNip());
            double actPmi = hA + hX + Math.log(xa.getNep() / r.getMaximumNip());
            double maxPmi = hA + hX + Math.log(Math.min(a.getNep(), x.getNep()) / r.getMaximumNip());
//            System.out.printf("PMI: observed %.5f\n", actPmi);
//            System.out.printf("PMI distribution: %.5f - %.5f -- %.5f -- %.5f - %.5f\n",
//                    pmis.getDouble(0),
//                    pmis.getDouble(repetitions / 4),
//                    pmis.getDouble(repetitions / 2),
//                    pmis.getDouble(repetitions * 3 / 4),
//                    pmis.getDouble(repetitions - 1)
//            );
//            System.out.printf("PMI distribution: %.5f < %.5f -- %.5f -- %.5f < %.5f\n",
//                    minPmi,
//                    pmis.getDouble(repetitions / 4),
//                    pmis.getDouble(repetitions / 2),
//                    pmis.getDouble(repetitions * 3 / 4),
//                    maxPmi
//            );
//            double meanPmi = 0d, variancePmi = 0d;
//            for (Double pmi : pmis) {
//                meanPmi += pmi;
//            }
//            meanPmi /= pmis.size();
//            for (Double pmi : pmis) {
//                variancePmi += (meanPmi - pmi) * (meanPmi - pmi);
//            }
//            variancePmi /= pmis.size();
//            System.out.printf(" Mean: %.5f, variance: %.5f, StdDev: %.5f\n", meanPmi, variancePmi, Math.sqrt(variancePmi));
//
//            System.out.printf("NPMI distribution: %.5f < %.5f -- %.5f -- %.5f < %.5f\n",
//                    -1d,
//                    npmis.getDouble(repetitions / 4),
//                    npmis.getDouble(repetitions / 2),
//                    npmis.getDouble(repetitions * 3 / 4),
//                    1d
//            );
//            double meanNpmi = 0d, varianceNpmi = 0d;
//            for (Double npmi : npmis) {
//                meanNpmi += npmi;
//            }
//            meanNpmi /= npmis.size();
//            for (Double npmi : npmis) {
//                varianceNpmi += (meanNpmi - npmi) * (meanNpmi - npmi);
//            }
//            varianceNpmi /= npmis.size();
//            System.out.printf(" Mean: %.5f, variance: %.5f, StdDev: %.5f\n", meanNpmi, varianceNpmi, Math.sqrt(varianceNpmi));

            return probGreaterMutualNip;
        };
    }

    public static PFDRater createNepRightTailAreaRater(int repetitions, boolean isVerbose) {
        return (x, a, xa, r) -> {

            // Materialize one vertical.
            PositionListIndex pli;
            int[] probingTable;
            if (x.getPositionListIndex().size() < a.getPositionListIndex().size()) {
                pli = x.getPositionListIndex();
                probingTable = a.getPositionListIndex().getProbingTable();
            } else {
                pli = a.getPositionListIndex();
                probingTable = x.getPositionListIndex().getProbingTable();
            }


            // Do the empirical experiment...
            double probGreaterCommonNep = 0d;
            DoubleArrayList neps = new DoubleArrayList(repetitions);
            double meanCommonNep = 0d;
            for (int i = 0; i < repetitions; i++) {
                // Shuffle the probing table and probe against it.
                Column.shuffle(probingTable, new Random());
                PositionListIndex intersection = pli.probe(probingTable);

                // Collect observations on t
                double randomCommonNep = InequalityPairs.nepToNip(intersection.getNip(), r.getNumRows());
                meanCommonNep += randomCommonNep;


                if (randomCommonNep >= xa.getNep()) {
                    probGreaterCommonNep += 1;
                }
                neps.add(randomCommonNep);
            }

            // Evaluate the observations.
            probGreaterCommonNep /= repetitions;
            meanCommonNep /= repetitions;
            neps.sort(Double::compareTo);
            double meanNep = 0d, varianceNep = 0d;
            for (Double nip : neps) {
                meanNep += nip;
            }
            meanNep /= neps.size();
            for (Double nep : neps) {
                varianceNep += (meanNep - nep) * (meanNep - nep);
            }
            varianceNep /= neps.size();
            double expectedNep = x.getNep() * a.getNep() / r.getMaximumNip();

            // Print feedback if requested.
            if (isVerbose) {
                System.out.printf(
                        "Empirical results on correlation of X=%s and A=%s (%,d repetitions):\n",
                        x, a, repetitions
                );
                System.out.printf("* N(X)=%,.0f\n" +
                                "* N(A)=%,.0f\n" +
                                "* N(R)=%,.0f\n" +
                                "* N(X;A)=%,.0f (+/-%,.0f) (random), %,.0f (%+,.0f) (expected), %,.0f (%+,.0f) (actual)\n",
                        x.getNep(), a.getNep(), r.getMaximumNip(),
                        meanCommonNep, Math.sqrt(varianceNep),
                        expectedNep, expectedNep - meanCommonNep,
                        xa.getNep(), xa.getNep() - meanCommonNep
                );
            }

            return probGreaterCommonNep;
        };
    }

    public static PFDRater createRNepRightTailAreaRater(int repetitions, boolean isVerbose) {
        return (x, a, xa, r) -> {

            // Materialize one vertical.
            PositionListIndex pli;
            int[] probingTable;
            if (x.getPositionListIndex().size() < a.getPositionListIndex().size()) {
                pli = x.getPositionListIndex();
                probingTable = a.getPositionListIndex().getProbingTable();
            } else {
                pli = a.getPositionListIndex();
                probingTable = x.getPositionListIndex().getProbingTable();
            }

            double xRNep = x.getPositionListIndex().getNumNonRedundantEP();
            double aRNep = a.getPositionListIndex().getNumNonRedundantEP();
            double xaRNep = xa.getPositionListIndex().getNumNonRedundantEP();


            // Do the empirical experiment...
            double probGreaterCommonRNep = 0d;
            DoubleArrayList rneps = new DoubleArrayList(repetitions);
            double meanCommonRNep = 0d;
            for (int i = 0; i < repetitions; i++) {
                // Shuffle the probing table and probe against it.
                Column.shuffle(probingTable, new Random());
                PositionListIndex intersection = pli.probe(probingTable);

                // Collect observations on t
                double randomCommonRNep = intersection.getNumNonRedundantEP();
                meanCommonRNep += randomCommonRNep;


                if (randomCommonRNep >= xaRNep) {
                    probGreaterCommonRNep += 1;
                }
                rneps.add(randomCommonRNep);
            }

            // Evaluate the observations.
            probGreaterCommonRNep /= repetitions;
            meanCommonRNep /= repetitions;
            rneps.sort(Double::compareTo);
            double meanRNep = 0d, varianceRNep = 0d;
            for (Double rnep : rneps) {
                meanRNep += rnep;
            }
            meanRNep /= rneps.size();
            for (Double rnep : rneps) {
                varianceRNep += (meanRNep - rnep) * (meanRNep - rnep);
            }
            varianceRNep /= rneps.size();

            // Print feedback if requested.
            if (isVerbose) {
                System.out.printf(
                        "Empirical results on correlation of X=%s and A=%s (%,d repetitions):\n",
                        x, a, repetitions
                );
                System.out.printf("* NR(X)=%,.0f\n" +
                                "* NR(A)=%,.0f\n" +
                                "* NR(R)=%,d\n" +
                                "* NR(X;A)=%,.0f (+/-%,.0f) (random), %,.0f (%+,.0f) (actual)\n",
                        xRNep, aRNep, r.getNumRows() - 1,
                        meanCommonRNep, Math.sqrt(varianceRNep),
                        xaRNep, xaRNep - meanCommonRNep
                );
            }

            return probGreaterCommonRNep;
        };
    }


    public static PFDRater createNipLeftTailAreaRater(int repetitions) {
        return (x, a, xa, r) -> {
            double probSmallerNip = 0d;
            double mutualInfo = x.getEntropy() + a.getEntropy() - xa.getEntropy();
            double mutualNip = x.getNip() + a.getNip() - xa.getNip();

            // Materialize one vertical.
            PositionListIndex pli;
            int[] probingTable;
            if (x.getPositionListIndex().size() < a.getPositionListIndex().size()) {
                pli = x.getPositionListIndex();
                probingTable = a.getPositionListIndex().getProbingTable();
            } else {
                pli = a.getPositionListIndex();
                probingTable = x.getPositionListIndex().getProbingTable();
            }

            // Shuffle and collect the data.
            double meanMI = 0d;
            double meanMN = 0d;
            for (int i = 0; i < repetitions; i++) {
                Column.shuffle(probingTable, new Random());
                PositionListIndex intersection = pli.probe(probingTable);
                double randomMutualInfo = a.getEntropy() + x.getEntropy() - intersection.getEntropy();
                double randomMutualNip = a.getNip() + x.getNip() - intersection.getNip();
                meanMI += randomMutualInfo;
                meanMN += randomMutualNip;
                if (randomMutualNip <= mutualNip) {
                    probSmallerNip += 1;
                }
            }
            meanMI /= repetitions;
            meanMN /= repetitions;
//            System.out.printf("Mean MI:  H(X)=%.4f, H(A)=%.4f, H(R)=%.4f, I(X;A)=%.4f, expected: %.4f, observed: %.4f\n",
//                    x.getEntropy(), a.getEntropy(), r.getMaximumEntropy(), meanMI,
//                    x.getEntropy() * a.getEntropy() / r.getMaximumEntropy(), mutualInfo
//            );
//            System.out.printf("Mean NIP: N(X)=%,.0f, N(A)=%,.0f, N(R)=%,.0f, N(X;A)=%,.0f, expected: %,.0f, observed: %,.0f\n",
//                    x.getNip(), a.getNip(), r.getMaximumNip(), meanMN,
//                    x.getNip() * a.getNip() / r.getMaximumNip(), mutualNip
//            );

            probSmallerNip /= repetitions;
            return probSmallerNip;
        };
    }

}
