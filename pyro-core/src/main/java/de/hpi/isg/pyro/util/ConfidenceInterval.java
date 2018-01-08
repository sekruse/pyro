package de.hpi.isg.pyro.util;

import java.io.Serializable;

/**
 * An interval consisting of two {@code double} values.
 */
public class ConfidenceInterval implements Serializable {

    private final double min, mean, max;


    public ConfidenceInterval(double value) {
        this(value, value, value);
    }

    public ConfidenceInterval(double min, double mean, double max) {
        if (!(min <= mean) || !(mean <= max)) {
            throw new IllegalArgumentException(String.format("Illegal interval bounds: (%e, %e, %e)", min, mean, max));
        }
        this.min = min;
        this.mean = mean;
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getMean() {
        return (this.min + this.max) / 2;
    }

    public double get() {
        assert this.min == this.mean && this.mean == this.max;
        return this.mean;
    }

    public ConfidenceInterval multiply(double scalar) {
        return new ConfidenceInterval(this.min * scalar, this.mean * scalar, this.max * scalar);
    }

    @Override
    public String toString() {
        return String.format("(%,.03f, %,.03f)", this.min, this.max);
    }

    public boolean isPoint() {
        return this.min == this.max;
    }
}
