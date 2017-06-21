package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.ColumnIdentifier;

import java.io.Serializable;
import java.util.BitSet;
import java.util.Comparator;
import java.util.function.ToDoubleFunction;

/**
 * A vertical describes a set of {@link Column}s from a common {@link RelationSchema}.
 */
public interface Vertical extends Serializable {

    /**
     * Retrieve the indices of the {@link Column}s wrt. their {@link RelationSchema}.
     *
     * @return the indices
     */
    BitSet getColumnIndices();

    /**
     * Check whether all {@link Column}s of the given instance are also found in this instance.
     *
     * @return the other instance
     */
    default boolean contains(Vertical that) {
        BitSet thisIndices = this.getColumnIndices();
        BitSet thatIndices = that.getColumnIndices();
        if (thisIndices.cardinality() < thatIndices.cardinality()) return false;
        for (int columnIndex = thatIndices.nextSetBit(0);
             columnIndex != -1;
             columnIndex = thatIndices.nextSetBit(columnIndex + 1)) {
            if (!thisIndices.get(columnIndex)) return false;
        }
        return true;
    }

    /**
     * Create a new instance that contains all {@link Column}s from this and {@code that} instance.
     *
     * @param that the {@link Column}s to project
     * @return the new instance
     */
    default Vertical union(Vertical that) {
        BitSet retainedColumnIndices = (BitSet) this.getColumnIndices().clone();
        retainedColumnIndices.or(that.getColumnIndices());
        return this.getSchema().getVertical(retainedColumnIndices);
    }

    /**
     * Create a new instance that contains all {@link Column}s that are in both this and {@code that} instance.
     *
     * @param that the {@link Column}s to project
     * @return the new instance
     */
    default Vertical project(Vertical that) {
        BitSet retainedColumnIndices = (BitSet) this.getColumnIndices().clone();
        retainedColumnIndices.and(that.getColumnIndices());
        return this.getSchema().getVertical(retainedColumnIndices);
    }

    /**
     * Create a new instance that contains all {@link Column}s from this instance without those in {@code that} instance.
     *
     * @param that the {@link Column}s to remove
     * @return the new instance
     */
    default Vertical without(Vertical that) {
        BitSet retainedColumnIndices = (BitSet) this.getColumnIndices().clone();
        retainedColumnIndices.andNot(that.getColumnIndices());
        return this.getSchema().getVertical(retainedColumnIndices);
    }

    /**
     * Creates a new instance that consists of all {@link Column}s that are <b>not</b> comprised in this instance.
     *
     * @return the inverted instance w.r.t. the {@link #getSchema() schema}
     */
    default Vertical invert() {
        BitSet flippedIndices = ((BitSet) this.getColumnIndices().clone());
        flippedIndices.flip(0, this.getSchema().getNumColumns());
        return this.getSchema().getVertical(flippedIndices);
    }

    /**
     * Creates a new instance that consists of all {@link Column}s that are either comprised in the {@code scope}
     * <b>or</b> in this instance.
     *
     * @return the inverted instance w.r.t. the {@code scope}
     */
    default Vertical invert(Vertical scope) {
        BitSet flippedIndices = ((BitSet) this.getColumnIndices().clone());
        flippedIndices.xor(scope.getColumnIndices());
        return this.getSchema().getVertical(flippedIndices);
    }

    RelationSchema getSchema();

    static Vertical emptyVertical(RelationSchema relation) {
        return new Vertical() {

            private final BitSet columnIndices = new BitSet(0);

            @Override
            public BitSet getColumnIndices() {
                return this.columnIndices;
            }

            public RelationSchema getSchema() {
                return relation;
            }

            @Override
            public String toString() {
                return "[]";
            }
        };
    }

    default int getArity() {
        return this.getColumnIndices().cardinality();
    }


    default Column[] getColumns() {
        BitSet columnIndices = this.getColumnIndices();
        RelationSchema relation = getSchema();
        Column[] columns = new Column[columnIndices.cardinality()];
        for (int index = columnIndices.nextSetBit(0), i = 0;
             index != -1;
             index = columnIndices.nextSetBit(index + 1), i++) {
            columns[i] = relation.getColumns().get(index);
        }
        return columns;
    }

    default Vertical[] getParents() {
        if (this.getArity() < 2) return new Vertical[0];
        Vertical[] parents = new Vertical[this.getArity()];
        BitSet columnIndices = (BitSet) this.getColumnIndices().clone();
        int i = 0;
        for (int columnIndex = columnIndices.nextSetBit(0);
             columnIndex != -1;
             columnIndex = columnIndices.nextSetBit(columnIndex + 1)) {
            columnIndices.clear(columnIndex);
            parents[i++] = this.getSchema().getVertical((BitSet) columnIndices.clone());
            columnIndices.set(columnIndex);
        }
        return parents;
    }

    /**
     * Convert this instance into a {@link de.metanome.algorithm_integration.ColumnCombination}.
     *
     * @return the converted instance
     */
    default de.metanome.algorithm_integration.ColumnCombination toMetanomeColumnCombination() {
        ColumnIdentifier[] columnIdentifiers = new ColumnIdentifier[this.getArity()];
        int i = 0;
        for (Column lhsColumn : this.getColumns()) {
            columnIdentifiers[i++] = lhsColumn.toMetanomeColumnIdentifier();
        }
        return new de.metanome.algorithm_integration.ColumnCombination(columnIdentifiers);
    }

    class Metric<T> {

        private final Vertical vertical;

        private final T value;

        public Metric(Vertical vertical, T value) {
            this.vertical = vertical;
            this.value = value;
        }

        public static <T> Comparator<Metric<T>> createComparator(ToDoubleFunction<T> valueExtractor) {
            return Comparator.comparingDouble(m -> valueExtractor.applyAsDouble(m.value));
        }

        public static <T> Comparator<Metric<T>> createComparator(ToDoubleFunction<T> f1, ToDoubleFunction<T> f2) {
            return Comparator.<Metric<T>>comparingDouble(m -> f1.applyAsDouble(m.value))
                    .thenComparingDouble(m -> f2.applyAsDouble(m.value));
        }

        public Vertical getVertical() {
            return vertical;
        }

        public T getValue() {
            return value;
        }
    }

    Comparator<Vertical> lexicographicComparator = (v1, v2) -> {
        int result = Integer.compare(v1.getArity(), v2.getArity());
        if (result != 0) return result;

        for (int i1 = v1.getColumnIndices().nextSetBit(0), i2 = v2.getColumnIndices().nextSetBit(0);
             i1 != -1;
             i1 = v1.getColumnIndices().nextSetBit(i1 + 1), i2 = v2.getColumnIndices().nextSetBit(i2 + 1)) {
            result = Integer.compare(i1, i2);
            if (result != 0) return result;
        }

        return 0;
    };
}