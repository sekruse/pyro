package de.hpi.isg.pyro.significance.histograms;

import java.io.PrintStream;
import java.util.function.DoubleFunction;
import java.util.function.Function;

/**
 * Defines the buckets of a histogram.
 */
public class HistogramDomain {

    /**
     * Defines bucket boundaries.
     * <p>
     * <p>Below the first value {@code b}, there is the bucket with values strictly smaller than {@code b}.</p>
     * <p>Two adjacent values {@code (b, c} define the buckets in the interval {@code [b, c[}.</p>
     * <p>The last value {@code c} defines the bucket with values greater or equal to {@code c}.</p>
     */
    private final double[] bucketBoundaries;

    public HistogramDomain(double[] bucketBoundaries) {
        this.bucketBoundaries = bucketBoundaries;
    }

    public double[] getBoundaries() {
        return this.bucketBoundaries;
    }

    public void plotBucketTitlesAsCsv(PrintStream out) {
        out.printf("\"x < %,.3f\";", this.bucketBoundaries[0]);
        for (int i = 0; i < bucketBoundaries.length - 1; i++) {
            double lowerBound = bucketBoundaries[i];
            double upperBound = bucketBoundaries[i + 1];
            out.printf("\"%,.3f <= x < %,.3f\";", lowerBound, upperBound);
        }
        out.printf("\"%,.3f <= x\"\n", this.bucketBoundaries[this.bucketBoundaries.length - 1]);
    }

    public void plotBucketTitlesAsCsv(PrintStream out,
                                      DoubleFunction<String> firstBucketFormatter,
                                      Function<double[], String> middleBucketFormatter,
                                      DoubleFunction<String> lastBucketFormatter) {
        out.printf("\"%s\";", firstBucketFormatter.apply(this.bucketBoundaries[0]));
        for (int i = 0; i < bucketBoundaries.length - 1; i++) {
            double lowerBound = bucketBoundaries[i];
            double upperBound = bucketBoundaries[i + 1];
            out.printf("\"%s\";", middleBucketFormatter.apply(new double[]{lowerBound, upperBound}));
        }
        out.printf("\"%s\"\n", lastBucketFormatter.apply(this.bucketBoundaries[this.bucketBoundaries.length - 1]));
    }
}
