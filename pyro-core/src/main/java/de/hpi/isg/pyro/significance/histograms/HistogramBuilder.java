package de.hpi.isg.pyro.significance.histograms;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleListIterator;

import java.io.PrintStream;
import java.util.Arrays;

/**
 * Utility to collect numerical data and later build histograms from it.
 */
public class HistogramBuilder implements CsvPrintableFunction {

    private final DoubleArrayList observedValues;

    private boolean isObservationsSorted;


    public HistogramBuilder() {
        this.observedValues = new DoubleArrayList();
        this.isObservationsSorted = true;
    }

    public void add(double value) {
        this.observedValues.add(value);
        this.isObservationsSorted = false;
    }

    public void addAll(HistogramBuilder that) {
        this.observedValues.addAll(that.observedValues);
        this.isObservationsSorted = false;
    }

    private void ensureObservationsSorted() {
        if (this.isObservationsSorted) return;
        Arrays.sort(this.observedValues.elements(), 0, this.observedValues.size());
        this.isObservationsSorted = true;
    }

    public double getLeftTailbound(double value) {
        this.ensureObservationsSorted();
        double numLeftTailObservations = 0;
        int index = Arrays.binarySearch(this.observedValues.elements(), 0, this.observedValues.size(), value);
        if (index < 0) {
            // The original value was established randomly.
            numLeftTailObservations = -index - 1;
        } else {
            // Scan to the right-most occurrence of the original value.
            while (index < this.observedValues.size() - 1 && this.observedValues.getDouble(index + 1) == value) {
                index++;
            }
            numLeftTailObservations = index + 1;
        }

        return numLeftTailObservations / this.observedValues.size();
    }

    public double getRightTailbound(double value) {
        this.ensureObservationsSorted();
        double numRightTailObservations = 0;
        int index = Arrays.binarySearch(this.observedValues.elements(), 0, this.observedValues.size(), value);
        if (index < 0) {
            // The original value was established randomly.
            numRightTailObservations = this.observedValues.size() - (-index - 1);
        } else {
            // Scan to the left-most occurrence of the original value.
            while (index > 0 && this.observedValues.getDouble(index - 1) == value) {
                index--;
            }
            numRightTailObservations = this.observedValues.size() - index;
        }

        return numRightTailObservations / this.observedValues.size();
    }

    public double getSmallestFiniteValue() {
        this.ensureObservationsSorted();
        for (DoubleListIterator iterator = this.observedValues.iterator(); iterator.hasNext(); ) {
            double v = iterator.nextDouble();
            if (Double.isFinite(v)) return v;
        }
        return Double.NaN;
    }

    public double getGreatestFiniteValue() {
        this.ensureObservationsSorted();
        for (DoubleListIterator iterator = this.observedValues.listIterator(this.observedValues.size()); iterator.hasPrevious(); ) {
            double v = iterator.previous();
            if (Double.isFinite(v)) return v;
        }
        return Double.NaN;
    }

    @Override
    public void plotAgainstAsCsv(HistogramDomain domain, PrintStream out) {
        this.ensureObservationsSorted();

        int lastIndex = 0;
        int index = 0;
        for (int bucketIndex = 0; bucketIndex < domain.getBoundaries().length; bucketIndex++) {
            // Scan forward until we hit the end of our data or find an element that does not belong in our bucket.
            double boundary = domain.getBoundaries()[bucketIndex];
            while (index < this.observedValues.size() && this.observedValues.getDouble(index) < boundary) {
                index++;
            }
            out.print(index - lastIndex);
            out.print(';');
            lastIndex = index;
        }
        out.print(this.observedValues.size() - index);
        out.print('\n');
    }

    public static HistogramDomain buildEquiwidthDomain(int numBuckets, HistogramBuilder... histogramBuilders) {
        final double epsilon = 0.001;

        if (histogramBuilders.length == 0) {
            throw new IllegalArgumentException();
        }

        double minValue = Double.POSITIVE_INFINITY, maxValue = Double.NEGATIVE_INFINITY;
        for (HistogramBuilder histogramBuilder : histogramBuilders) {
            if (histogramBuilder.observedValues.isEmpty()) continue;
            histogramBuilder.ensureObservationsSorted();
            double value = histogramBuilder.getSmallestFiniteValue();
            if (!Double.isNaN(value)) minValue = Math.min(minValue, value);
            value = histogramBuilder.getGreatestFiniteValue();
            if (!Double.isNaN(value)) maxValue = Math.max(maxValue, value);
        }

        if (minValue == maxValue) return new HistogramDomain(new double[]{minValue});
        if (minValue > maxValue) return new HistogramDomain(new double[]{0});

        maxValue += epsilon; // offset to include the maximum value in the last bucket.
        double delta = (maxValue - minValue) / numBuckets;
        double[] bucketBoundaries = new double[numBuckets + 1];
        for (int i = 0; i < bucketBoundaries.length - 1; i++) {
            bucketBoundaries[i] = minValue + delta * i;
        }
        bucketBoundaries[bucketBoundaries.length - 1] = maxValue;

        return new HistogramDomain(bucketBoundaries);
    }

    public static HistogramDomain buildEquidepthDomain(int numBuckets, HistogramBuilder... histogramBuilders) {
        final double epsilon = 0.001;

        if (histogramBuilders.length == 0) {
            throw new IllegalArgumentException();
        }

        DoubleArrayList allElements = new DoubleArrayList();
        for (HistogramBuilder histogramBuilder : histogramBuilders) {
            allElements.addAll(histogramBuilder.observedValues);
        }
        Arrays.sort(allElements.elements(), 0, allElements.size());

        if (allElements.isEmpty()) return new HistogramDomain(new double[]{0});

        DoubleArrayList boundaries = new DoubleArrayList(numBuckets + 1);
        boundaries.add(allElements.getDouble(0));
        double baseValue = boundaries.getDouble(0);
        int lastIndex = 0;
        for (int i = 1; i < numBuckets; i++) {
            int nextIndex = i * allElements.size() / numBuckets;
            if (nextIndex <= lastIndex) continue;
            while (nextIndex + 1 < allElements.size() && baseValue == allElements.getDouble(nextIndex)) {
                nextIndex++;
            }
            if (nextIndex + 1 == allElements.size()) break;
            boundaries.add(allElements.getDouble(nextIndex));
            lastIndex = nextIndex;
        }
        boundaries.add(allElements.getDouble(allElements.size() - 1) + epsilon);

        return new HistogramDomain(boundaries.toDoubleArray());
    }
}
