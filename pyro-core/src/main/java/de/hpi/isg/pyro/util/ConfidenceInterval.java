package de.hpi.isg.pyro.util;

/**
 * An interval consisting of two {@code double} values.
 */
public class ConfidenceInterval {

    private final double min, max;

    public ConfidenceInterval(double min, double max) {
        if (!(min <= max)) {
            throw new IllegalArgumentException(String.format("Illegal interval bounds: (%e, %e)", min, max));
        }
        this.min = min;
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
        assert this.min == this.max;
        return this.min;
    }

    public ConfidenceInterval multiply(double scalar) {
        return new ConfidenceInterval(this.min * scalar, this.max * scalar);
    }

    @Override
    public String toString() {
        return String.format("(%,.03f, %,.03f)", this.min, this.max);
    }

    public boolean isPoint() {
        return this.min == this.max;
    }
}
