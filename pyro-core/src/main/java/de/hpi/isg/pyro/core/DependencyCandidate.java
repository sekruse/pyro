package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.AgreeSetSample;
import de.hpi.isg.pyro.util.ConfidenceInterval;

import java.util.BitSet;
import java.util.Comparator;
import java.util.Objects;

/**
 * This class describes a dependency candidate (in the light of some {@link DependencyStrategy}) including its
 * (estimated) error. Dependency candidates are ordered by their {@link #error} (ascending) and by the arity of the {@link #vertical} (descending).
 */
public class DependencyCandidate implements Comparable<DependencyCandidate> {

    public static Comparator<DependencyCandidate> arityComparator = (tc1, tc2) -> {
        // Primarily order by the arity.
        int result = Integer.compare(tc1.vertical.getArity(), tc2.vertical.getArity());
        if (result != 0) return result;

        // Use the error to break ties.
        return Double.compare(tc1.error.getMean(), tc2.error.getMean());
    };

    public static Comparator<DependencyCandidate> meanErrorComparator = (tc1, tc2) -> {
        // Primarily order by the error.
        int result = Double.compare(tc1.error.getMean(), tc2.error.getMean());
        if (result != 0) return result;

        // Use the arity to break ties.
        return Integer.compare(tc1.vertical.getArity(), tc2.vertical.getArity());
    };

    public static Comparator<DependencyCandidate> minErrorComparator = (tc1, tc2) -> {
        // Primarily order by the error.
        int result = Double.compare(tc1.error.getMin(), tc2.error.getMin());
        if (result != 0) return result;

        // Use the arity to break ties.
        return Integer.compare(tc1.vertical.getArity(), tc2.vertical.getArity());
    };

    /**
     * The {@link Vertical} to visit represented by this instance.
     */
    public final Vertical vertical;

    /**
     * An estimate of the key or FD error.
     */
    public ConfidenceInterval error;

    /**
     * The {@link AgreeSetSample} from which this instance has been created.
     */
    public AgreeSetSample agreeSetSample;

    public DependencyCandidate(Vertical vertical, ConfidenceInterval error, AgreeSetSample agreeSetSample) {
        this.vertical = vertical;
        this.error = error;
        this.agreeSetSample = agreeSetSample;
    }

    @Override
    public int compareTo(DependencyCandidate that) {
        // Primarily order by the error.
        int result = Double.compare(this.error.getMean(), that.error.getMean());
        if (result != 0) return result;

        // Use the arity to break ties.
        result = Integer.compare(that.vertical.getArity(), this.vertical.getArity());
        if (result != 0) return result;

        // Finally, apply a lexicographical comparison to remove duplicates.
        BitSet thisColumns = this.vertical.getColumnIndices();
        BitSet thatColumns = that.vertical.getColumnIndices();
        for (int a = thisColumns.nextSetBit(0), b = thatColumns.nextSetBit(0);
             a != -1;
             a = thisColumns.nextSetBit(a + 1), b = thatColumns.nextSetBit(b + 1)) {
            if (a < b) return -1;
            else if (a > b) return 1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return String.format("candidate %s (err=%s, from %s)", this.vertical, this.error, this.agreeSetSample);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DependencyCandidate that = (DependencyCandidate) o;
        return Objects.equals(vertical, that.vertical) &&
                Objects.equals(error, that.error) &&
                Objects.equals(agreeSetSample, that.agreeSetSample);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertical, error, agreeSetSample);
    }

    /**
     * Tells whether the {@link #error} of this instance is exact rather than an estimate.
     *
     * @return whether the {@link #error} is exact
     */
    public boolean isExact() {
        return this.error.isPoint() && (this.agreeSetSample == null || this.agreeSetSample.isExact());
    }
}