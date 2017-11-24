package de.hpi.isg.pyro.util;

import java.util.ArrayList;
import java.util.List;

/**
 * This utility offers calculations for various mathematical/combinatorial/probabilistic/heuristic ideas.
 */
public class HyperGeometricDistributions {

    /**
     * The accuracy within which we would like to calculate probabilities.
     */
    private static final double EPSILON = 1E-5;

    private static final double LOG_EPSILON = Math.log(1.01); // Reduce error (H0/H1) to 1%.

    private static final short MAX_ESTIMATION_RECURSIONS = 5;

    private static final double MIN_LOG = Math.log(Double.MIN_VALUE);

    private static final long SCALE_GOAL = 10000;

    /**
     * Calculate the hypergeometric distribution.
     * <p>More specifically, we estimate the following formula (w.l.k.g. {@code n <= K}):</p>
     * <p>(n-k, k)! * (K-k, (N-n)-(K-k))! / (K, N-K)!</p>
     *
     * @param n number of draws
     * @param K number of overall successes
     * @param k number of successful draws
     * @param N number of overall elements
     * @return the probability of {@code k} successful draws
     */
    public static double pmf(long n, long K, long k, long N) {
        // Handle some special cases.
        if (n == 0) {
            return k == 0 ? 1d : 0d;
        }

        if (n > K) return pmf(K, n, k, N);

        // The base idea is to specify for each of the multinomial coefficients the factors they contribute to the
        // numerator and the denominator.
        // Then, we process them in an alternating fashion to avoid overflows.
        List<FactorQueue> numeratorFactorQueues = new ArrayList<>();
        List<FactorQueue> denominatorFactorQueues = new ArrayList<>();
        addMultinomialCoefficientFactorQueues(n - k, k, numeratorFactorQueues, denominatorFactorQueues);
        addMultinomialCoefficientFactorQueues(K - k, (N - n) - (K - k), numeratorFactorQueues, denominatorFactorQueues);
        addMultinomialCoefficientFactorQueues(K, N - K, denominatorFactorQueues, numeratorFactorQueues);

        // Process the queue in an interleaved fashion.
        double accu = 1d;
        while (!numeratorFactorQueues.isEmpty() && !denominatorFactorQueues.isEmpty()) {
            if (accu <= 10000000000d) {
                final FactorQueue queue = numeratorFactorQueues.get(0);
                if (queue.isEmpty()) {
                    numeratorFactorQueues.remove(0);
                } else {
                    accu *= queue.pollUpper();
                }
            } else {
                final FactorQueue queue = denominatorFactorQueues.get(0);
                if (queue.isEmpty()) {
                    denominatorFactorQueues.remove(0);
                } else {
                    accu /= queue.pollUpper();
                }
            }

            if (accu == 0) {
//                System.out.printf("calculate(%d, %d, %d, %d) = 0 (a).\n", n, K, k, N);
                return 0;
            }
        }

        // Process any queue remainders.
        while (!numeratorFactorQueues.isEmpty()) {
            final FactorQueue queue = numeratorFactorQueues.get(0);
            if (queue.isEmpty()) {
                numeratorFactorQueues.remove(0);
            } else {
                accu *= queue.pollUpper();
            }

            if (accu == 0) {
//                System.out.printf("calculate(%d, %d, %d, %d) = 0 (b).\n", n, K, k, N);
                return 0;
            }
        }
        while (!denominatorFactorQueues.isEmpty()) {
            final FactorQueue queue = denominatorFactorQueues.get(0);
            if (queue.isEmpty()) {
                denominatorFactorQueues.remove(0);
            } else {
                accu /= queue.pollUpper();
            }

            if (accu == 0) {
//                System.out.printf("calculate(%d, %d, %d, %d) = 0 (c).\n", n, K, k, N);
                return 0;
            }
        }

//        System.out.printf("calculate(%d, %d, %d, %d) = %e (d).\n", n, K, k, N, accu);
        return accu;
    }

    /**
     * Estimate the natural logarithm of the probability of having {@code k} or less successes. That is, estimate
     * the area of the left tail PMF.
     *
     * @param n number of draws
     * @param K number of overall successes
     * @param k maximum number of successful draws
     * @param N number of overall elements
     * @return the probability of {@code k} successful draws
     */
    public static double estimateLogLeftTailArea(long n, long K, long k, long N) {
        double mean = n * (double) K / N;
        if (k <= mean) {
            long kMin = minSuccesses(n, K, N);
            return estimateLogArea(n, K, kMin, k, N);
        } else {
            // Don't worry too much about precision in this case.
            long kMax = maxSuccesses(n, K);
            if (k == kMax) return 0d; // log(1) = 0
            double logRightArea = estimateLogArea(n, K, k + 1, kMax, N);
            return flip(logRightArea);
        }
    }

    /**
     * Estimate the natural logarithm of the probability of having {@code k} or more successes. That is, estimate
     * the area of the right tail PMF.
     *
     * @param n number of draws
     * @param K number of overall successes
     * @param k minimum number of successful draws
     * @param N number of overall elements
     * @return the probability of {@code k} successful draws
     */
    public static double estimateLogRightTailArea(long n, long K, long k, long N) {
        double mean = mean(n, K, N);
        if (k >= mean) {
            long kMax = maxSuccesses(n, K);
            return estimateLogArea(n, K, k, kMax, N);
        } else {
            // Don't worry too much about precision in this case.
            long kMin = minSuccesses(n, K, N);
            if (kMin == k) return 0d; // log(1) = 0
            double logLeftArea = estimateLogArea(n, K, kMin, k - 1, N);
            return flip(logLeftArea);
        }
    }

    /**
     * Calculates the logarithm of the complementary area (such that both areas approximately add up to 1).
     *
     * @param logBaseArea logarithm of the base area
     * @return the logarithm of the complementary area
     */
    private static double flip(double logBaseArea) {
        if (logBaseArea == 0) return Double.NEGATIVE_INFINITY;
        double baseArea = Math.exp(logBaseArea);
        return Math.log1p(-baseArea);
    }


    /**
     * Estimate the sum of the PMF function on a given interval. The PMF should monotonically increasing or decreasing
     * on the given interval.
     */
    private static double estimateLogArea(long n, long K, long kMin, long kMax, long N) {
        if (kMin > kMax)
            throw new IllegalArgumentException(String.format("Illegal interval for k: %d > %d", kMin, kMax));

        double logPLo = estimateLogPmf(n, K, kMin, N);
        double logPHi = estimateLogPmf(n, K, kMax, N);
        double logAreaEstimate = estimateLogArea(n, K, kMin, logPLo, kMax, logPHi, N, 0);
        if (logAreaEstimate > 0d) {
            // Catch estimation errors.
            logAreaEstimate = 0d;
        }
        return logAreaEstimate;
    }


    /**
     * Estimate the sum of the PMF function on a given interval. The PMF should monotonically increasing or decreasing
     * on the given interval.
     */
    private static double estimateLogArea(long n, long K, long kMin, double logPLo, long kMax, double logPHi, long N, int recursionDepth) {
//        System.out.printf("Estimating log area on [%d, %d].\n", kMin, kMax);
        if (recursionDepth >= MAX_ESTIMATION_RECURSIONS || logPHi == logPLo) {
            return Math.log(kMax - kMin + 1) + logPHi;
        }

        // Hypothesis 0: The area can be estimated as the area under a straight line between kMin and kMax.
        double logArea0 = addLogs(logPLo, logPHi) + Math.log(kMax - kMin + 1); // log(area0) = log((pLo+pHi) * (kMax-kMin+1))

        // Test: Check whether dividing up into two lines improves the result.
        long kMid = (kMin + kMax) / 2;
        double logPMid = estimateLogPmf(n, K, kMid, N);
        double logPMidP1 = estimateLogPmf(n, K, kMid + 1, N);

        double logArea1a = addLogs(logPLo, logPMid) + Math.log(kMid - kMin + 1); // log(area1) on [pLo, pMid]
        double logArea1b = addLogs(logPMidP1, logPHi) + Math.log(kMax - (kMid + 1) + 1); // log(area1) on ]pMid, pHi]
        double logArea1 = addLogs(logArea1a, logArea1b); // log(area1) = log(area1a + area1b)

        double logError = Math.abs(logArea0 - logArea1); // log(error) = log(area0 / area1)
//        System.out.printf("|%e - %e| = %e (%e / %e = %e)\n", logArea0, logArea1, logError, Math.exp(logArea0), Math.exp(logArea1), Math.exp(logError));
        if (logError > LOG_EPSILON) { // log(error) > log(epsilon)?
            return addLogs(
                    estimateLogArea(n, K, kMin, logPLo, kMid, logPMid, N, recursionDepth + 1),
                    estimateLogArea(n, K, kMid + 1, logPMidP1, kMax, logPHi, N, recursionDepth + 1)
            );
        } else {
            return logArea1;
        }
    }

    /**
     * Estimates the natural logarithm of the PMF of the hypergeometric distribution using Euler's estimation
     * of the logarithm of binomial coefficients.
     *
     * @param n number of draws
     * @param K number of overall successes
     * @param k number of successful draws
     * @param N number of overall elements
     * @return the probability of {@code k} successful draws
     */
    public static double estimateLogPmf(long n, long K, long k, long N) {
        return estimateLogBinomialCoefficient(K, k)
                + estimateLogBinomialCoefficient(N - K, n - k)
                - estimateLogBinomialCoefficient(N, n);
    }

    /**
     * Estimate the natural logarithm of a binomial coefficient using Stirling's approximation.
     *
     * @param n the <i>n</i> in <i>n-choose-k</i>
     * @param k the <i>k</i> in <i>n-choose-k</i>
     * @return the estimate
     */
    private static double estimateLogBinomialCoefficient(double n, double k) {
        return estimateLogFactorial(n) - estimateLogFactorial(k) - estimateLogFactorial(n - k);
    }

    /**
     * Equals {@code ln(2 * pi)}.
     */
    private static final double LOG_2PI = Math.log(2 * Math.PI);

    /**
     * Estimate the natural logarithm of a factorial using Stirling's approximation.
     *
     * @param n the operand
     * @return the estimate
     */
    private static double estimateLogFactorial(double n) {
        if (n == 0) return 0; // log(0!) = log(1) = 0
        if (n == 1) return 0; // log(1!) = log(1) = 1

        // We use n! ~ (2 pi n)^.5 * (n / e)^n
        // => log(n!) ~ 0.5 * log (2 pi n) + n * log n - n
        double logN = Math.log(n);
        return 0.5 * (LOG_2PI + logN) + n * (logN + 1);
    }

    /**
     * Calculate {@code log(a + b)}.
     *
     * @param logA {@code log(a)} with {@code a < 0}
     * @param logB {@code log(b)} with {@code a < b}
     * @return {@code log(a + b)} best effort calculation
     */
    private static double addLogs(double logA, double logB) {
        // Make sure that we rather end up with 0 than infinity for log(B/A).
        if (logA < logB) return addLogs(logB, logA);
        return logA + Math.log1p(Math.exp(logB - logA));

    }

    /**
     * Formats the natural logarithm of some number using the scientific notation (with three significant decimal places).
     *
     * @param lnX the natural logarithm of some number
     * @return the scientific notation ({@code <a>e<b>} where {@code 0 <= a <= 1} and {@code b} is some integer)
     */
    public static String formatLogScientific(double lnX) {
        if (Double.isNaN(lnX)) return "NaN";
        else if (lnX == Double.POSITIVE_INFINITY) return "+infinitity";
        else if (lnX == Double.NEGATIVE_INFINITY) return "0";

        // Format some ln(x) as "a * 10^b" (1 <= a < 10, b in Z)
        double lgAPlusB = lnX * Math.log10(Math.E); // = lg(a) + b
        long b = (long) Math.floor(lgAPlusB);
        double a = Math.pow(10, lgAPlusB - b);

        // Make sure, we are not formatting towards 10e***.
        if (a > 9.999) a = 9.999;
        return String.format("%.03fe%+d", a, b);
    }

    /**
     * Resolves the logarithm of a number between 0 and 1. In contrast to just using {@link Math#exp(double)}, some
     * additional checks are being made.
     *
     * @param lnX the logarithm of some number between 0 and 1
     * @return the number
     */
    public static double resolveLog(double lnX) {
        if (lnX == Double.NEGATIVE_INFINITY) return 0d;
        return Math.exp(lnX);
    }

    /**
     * Apply the natural logarithm to a number {@code <m>e<exp>}.
     */
    public static double toLog(double m, double exp) {
        return Math.log(m) + exp * Math.log(10);
    }


    public static double calculateRightTailPosition(long n, long K, long k, long N) {
        double maxk = maxSuccesses(n, K);
        double mean = mean(n, K, N);
        if (maxk <= mean) return 0d;
        return (k - mean) / (maxk - mean);
    }


    public static double calculateLeftTailPosition(long n, long K, long k, long N) {
        double mink = minSuccesses(n, K, N);
        double mean = mean(n, K, N);
        if (mink >= mean) return 0d;
        return (mean - k) / (mean - mink);
    }

    public static double mean(double n, double K, double N) {
        return n * K / N;
    }

    public static double variance(double n, double K, double N) {
        return mean(n, K, N) * (N - K) / N * (N - n) / (N - 1);
    }

    public static double stddev(double n, double K, double N) {
        return Math.sqrt(variance(n, K, N));
    }

    public static long maxSuccesses(long n, long K) {
        return Math.min(n, K);
    }


    public static long minSuccesses(long n, long K, long N) {
        return Math.max(0, n + K - N);
    }

    /**
     * Calculate the probability that two sets with cardinalities {@code a} and {@code b} from a universe of cardinality
     * {@code u} overlap in at least {@code o} elements.
     *
     * @param a cardinality of the first set
     * @param b cardinality of the second set
     * @param o the overlap
     * @param u cardinality of the universe
     * @return the probability of the overlap
     */
    public static double calculateAtLeastOverlapProbability(long a, long b, long o, long u) {
        double accu = 0;
        double maxOverlap = Math.min(a, b);
        while (o <= maxOverlap) {
            accu += pmf(a, b, o++, u);
        }
        return accu;
    }

    public static double estimateRightTailArea(long n, long K, long k, long N) {
        if (N == 0 || K == 0) return k <= 0 ? 1d : 0d;
        return k >= mean(n, K, N) ?
                estimateArea(n, K, k, maxSuccesses(n, K), N) :
                1 - estimateLeftTailArea(n, K, k - 1, N);
    }

    public static double estimateRightTailArea2(long n, long K, long k, long N) {
        if (N == 0 || K == 0) return k <= 0 ? 1d : 0d;
        k--;
        double p = K / (double) N;
        return 1 - NormalDistributions.cdf((k - n * p) / Math.sqrt(n * p * (1 - p)));
    }

    /**
     * Calculate the probability that two sets with cardinalities {@code a} and {@code b} from a universe of cardinality
     * {@code u} overlap in at most {@code o} elements.
     *
     * @param a cardinality of the first set
     * @param b cardinality of the second set
     * @param o the overlap
     * @param u cardinality of the universe
     * @return the probability of the overlap
     */
    public static double calculateAtMostOverlapProbability(long a, long b, long o, long u) {
        double accu = 0;
        double minOverlap = Math.max(0L, a + b - u);
        while (o >= minOverlap) {
            accu += pmf(a, b, o--, u);
        }
        return accu;
    }

    public static double estimateLeftTailArea(long n, long K, long k, long N) {
        if (N == 0 || K == 0) return k >= 0 ? 1d : 0d;
        return k <= mean(n, K, N) ?
                estimateArea(n, K, minSuccesses(n, K, N), k, N) :
                1 - estimateRightTailArea(n, K, k + 1, N);
    }

    public static double estimateLeftTailArea2(long n, long K, long k, long N) {
        if (N == 0 || K == 0) return k >= 0 ? 1d : 0d;
        double p = K / (double) N;
        return NormalDistributions.cdf((k - n * p) / Math.sqrt(n * p * (1 - p)));
    }


    /**
     * Calculate the probability that two sets with cardinalities {@code a} and {@code b} from a universe of cardinality
     * {@code u} overlap in at least {@code o} elements.
     *
     * @param a cardinality of the first set
     * @param b cardinality of the second set
     * @param u cardinality of the universe
     * @return the probability of the overlap
     */
    public static double estimateArea(long a, long b, long oMin, long oMax, long u) {
        // Scale down the problem.
        System.out.printf("Scaling down a=%d, b=%d, oMin=%d, oMax=%d, u=%d.\n", a, b, oMin, oMax, u);
        while (oMin > SCALE_GOAL || (oMin == 0 && oMax > SCALE_GOAL) || (oMax == 0 && Math.min(a, b) > SCALE_GOAL)) {
            a /= 2;
            b /= 2;
            oMin /= 2;
            oMax /= 2;
            u /= 2;
        }
        System.out.printf("Scaled down to a=%d, b=%d, oMin=%d, oMax=%d, u=%d.\n", a, b, oMin, oMax, u);

        // Calculate the probabilities of the interval border.
        double pMin = pmf(a, b, oMin, u);
        double pMax = pmf(a, b, oMax, u);

        return estimateAreaAux(a, b, oMin, pMin, oMax, pMax, u, EPSILON);
    }

    private static double estimateAreaAux(long a, long b, long oMin, double pMin, long oMax, double pMax, long u, double epsilon) {
        if (oMin == oMax) {
            return pMin;
        }

        // Exit strategy for small intervals.
        if (oMin + 1 == oMax) {
            return pMax + pMin;
        }

        // Estimate the interval probability assuming a linear function.
        double hypothesis0 = (pMin + pMax) / 2 * (oMax - oMin + 1);

        // Calculate the probability in the middle of the interval.
        long oMiddle = (oMin + oMax) / 2;
        double pMiddle = pmf(a, b, oMiddle, u);

        // Formulate a new hypothesis using the middle point.
        double hypothesis1 = (pMin + pMiddle) / 2 * (oMiddle - oMin + 1) +
                (pMiddle + pMax) / 2 * (oMax - oMiddle);

        // If the new hypothesis did not change much, then return it.
        if (Math.abs(hypothesis0 - hypothesis1) < epsilon) return hypothesis1;

        // Otherwise refine recursively.
        return estimateAreaAux(a, b, oMin, pMin, oMiddle, pMiddle, u, 2 * epsilon) +
                estimateAreaAux(a, b, oMiddle + 1, pMiddle, oMax, pMax, u, 2 * epsilon);

    }


    private static void addMultinomialCoefficientFactorQueues(
            long a, long b,
            List<FactorQueue> numeratorFactorQueues,
            List<FactorQueue> denominatorFactorQueues) {
        if (a > b) {
            addMultinomialCoefficientFactorQueues(b, a, numeratorFactorQueues, denominatorFactorQueues);
        } else {
            numeratorFactorQueues.add(new FactorQueue(b + 1, a + b));
            denominatorFactorQueues.add(new FactorQueue(2, a));
        }

    }

    private static class FactorQueue {

        private long upperFactor, lowerFactor;

        public FactorQueue(long lowerFactor, long upperFactor) {
            this.upperFactor = upperFactor;
            this.lowerFactor = lowerFactor;
        }

        public boolean isEmpty() {
            return this.lowerFactor > this.upperFactor;
        }

        public long pollUpper() {
            if (this.isEmpty()) throw new IllegalStateException();
            return this.upperFactor--;
        }

        public long pollLower() {
            if (this.isEmpty()) throw new IllegalStateException();
            return this.lowerFactor--;
        }
    }

}
