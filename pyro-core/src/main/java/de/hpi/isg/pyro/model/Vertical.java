package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.PLICache;
import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.ColumnIdentifier;

import java.util.*;
import java.util.function.ToDoubleFunction;

/**
 * A vertical is an abstract description of one or more {@link Column}s.
 */
public interface Vertical {

    /**
     * Retrieve the indices of the {@link Column}s wrt. their {@link Relation}.
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
     * <i>Optional operation.</i> Set the {@link PositionListIndex} for this instance.
     *
     * @param pli the {@link PositionListIndex}
     */
    default void setPositionListIndex(PositionListIndex pli) {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieve the {@link PositionListIndex} for this instance.
     * <p>In contrast to {@link #tryGetPositionListIndex()}, the {@link PositionListIndex} will be calculated
     * if it is not available.</p>
     *
     * @return the {@link PositionListIndex}
     */
    PositionListIndex getPositionListIndex();

    /**
     * Retrieve the {@link PositionListIndex} for this instance using the {@link PLICache} if necessary.
     *
     * @param pliCache can provide the {@link PositionListIndex} and cache it
     * @return the {@link PositionListIndex}
     */
    default PositionListIndex getPositionListIndex(PLICache pliCache) {
        return pliCache.getPositionListIndex(this);
    }

    /**
     * Retrieves the {@link PositionListIndex} for this instance if available.
     *
     * @return the {@link PositionListIndex} or {@code null} if it is not available
     */
    default PositionListIndex tryGetPositionListIndex() {
        return this.getPositionListIndex();
    }

    /**
     * Retrieve the entropy of this instance.
     *
     * @return the entropy
     */
    default double getEntropy() {
        return this.getPositionListIndex().getEntropy();
    }

    /**
     * Retrieve the number of inequality pairs in this instance.
     *
     * @return the number of inequality pairs (NIP)
     */
    default double getNip() {
        return this.getPositionListIndex().getNip();
    }


    /**
     * Retrieve the number of equality pairs in this instance.
     *
     * @return the number of equality pairs (NEP)
     */
    default double getNep() {
        return this.getRelation().getMaximumNip() - this.getPositionListIndex().getNip();
    }

    default Vertical union(Vertical that) {
        BitSet allColumnIndices = (BitSet) this.getColumnIndices().clone();
        allColumnIndices.or(that.getColumnIndices());
        return this.getRelation().getVertical(allColumnIndices);
    }

    default Vertical project(Vertical that) {
        BitSet retainedColumnIndices = (BitSet) this.getColumnIndices().clone();
        retainedColumnIndices.and(that.getColumnIndices());
        return this.getRelation().getVertical(retainedColumnIndices);
    }

    default Vertical without(Vertical that) {
        BitSet retainedColumnIndices = (BitSet) this.getColumnIndices().clone();
        retainedColumnIndices.andNot(that.getColumnIndices());
        return this.getRelation().getVertical(retainedColumnIndices);
    }

    /**
     * Creates a new instance that consists of all {@link Column}s that are <b>not</b> comprised in this instance.
     *
     * @return the inverted instance
     */
    default Vertical invert() {
        BitSet flippedIndices = ((BitSet) this.getColumnIndices().clone());
        flippedIndices.flip(0, this.getRelation().getNumColumns());
        return this.getRelation().getVertical(flippedIndices);
    }

    /**
     * Creates a new instance that consists of all {@link Column}s that are either comprised in the {@code scope}
     * <b>or</b> in this instance.
     *
     * @return the inverted instance
     */
    default Vertical invert(Vertical scope) {
        BitSet flippedIndices = ((BitSet) this.getColumnIndices().clone());
        flippedIndices.xor(scope.getColumnIndices());
        return this.getRelation().getVertical(flippedIndices);
    }

    Relation getRelation();

    default double getKeyness() {
        return this.getEntropy() / this.getRelation().getMaximumEntropy();
    }

    static Vertical emptyVertical(Relation relation) {
        return new Vertical() {

            private final BitSet columnIndices = new BitSet(0);


            @Override
            public BitSet getColumnIndices() {
                return this.columnIndices;
            }

            @Override
            public PositionListIndex getPositionListIndex() {
                throw new UnsupportedOperationException();
            }

            @Override
            public double getNip() {
                return 0;
            }

            @Override
            public double getNep() {
                return this.getRelation().getMaximumNip();
            }

            @Override
            public double getEntropy() {
                return 0d;
            }

            @Override
            public PositionListIndex tryGetPositionListIndex() {
                return null;
            }

            @Override
            public Relation getRelation() {
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
        Relation relation = getRelation();
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
            parents[i++] = this.getRelation().getVertical((BitSet) columnIndices.clone());
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

    default <T> Metric<T> createMetric(T value) {
        return new Metric<>(this, value);
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