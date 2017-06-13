package de.hpi.isg.pyro.util;

/**
 * This class provides utilities to operate with normal distributions.
 */
public class NormalDistributions {

    public static double cdf(double x) {
        return estimatedCdf(x, 1e-5);
    }

    public static double estimatedCdf(double x, double precision) {
        double xSquared = x * x;
        double f = Math.exp(-xSquared / 2) / Math.sqrt(2 * Math.PI);
        if (f == 0) return x < 0 ? 0d : 1d;
        precision /= f;
        double delta = x;
        double sum = x;
        int i = 0;
        while (delta > precision) {
            delta *= xSquared / (2 * ++i + 1);
            sum += delta;
        }
        return 0.5 + f * sum;
    }

}
