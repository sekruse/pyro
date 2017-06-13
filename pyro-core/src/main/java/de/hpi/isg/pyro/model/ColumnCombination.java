package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.PositionListIndex;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Objects;

/**
 * Models a combination of {@link Column}s.
 */
public class ColumnCombination implements Vertical {

    private final BitSet columnIndices;

    private Reference<PositionListIndex> positionListIndexCache;

    /**
     * This member should not be queried and is only used to "lock in" the contents of the {@link #positionListIndexCache}.
     */
    private PositionListIndex positionListIndexLock;

    private final Relation relation;

    public ColumnCombination(BitSet columnIndices, Relation relation) {
        this.columnIndices = columnIndices;
        this.relation = relation;
    }

    @Override
    public PositionListIndex tryGetPositionListIndex() {
        return this.positionListIndexCache != null ? this.positionListIndexCache.get() : null;
    }

    public PositionListIndex getPositionListIndex() {
        PositionListIndex pli = this.tryGetPositionListIndex();
        if (pli == null) {
            pli = this.recalculatePositionListIndex();
            this.positionListIndexCache = new SoftReference<>(pli);
//            System.out.printf("Recalculating PLI for %s.\n", this);
        }
        return pli;
    }

    /**
     * Recalculate the {@link PositionListIndex} for this index from scratch.
     */
    private PositionListIndex recalculatePositionListIndex() {
        System.out.printf("Warning: Calculating PLI for %s from bare columns.\n", this);

        // Retrieve the PLIs for the columns.
        PositionListIndex[] columnPLIs = new PositionListIndex[columnIndices.cardinality()];
        int i = 0;
        for (int columnIndex = this.columnIndices.nextSetBit(0);
             columnIndex != -1;
             columnIndex = this.columnIndices.nextSetBit(columnIndex + 1)) {
            columnPLIs[i++] = this.relation.getColumns().get(columnIndex).getPositionListIndex();
        }

        // Optimization: Sort the PLIs by their number of entries.
        Arrays.sort(columnPLIs, Comparator.comparing(PositionListIndex::size));

        // Perform the intersects one after another.
        PositionListIndex result = columnPLIs[0];
        for (i = 1; i < columnPLIs.length; i++) {
            result = result.intersect(columnPLIs[i]);
        }

        return result;
    }

    /**
     * Set the {@link PositionListIndex} for this instance explicitly.
     *
     * @param positionListIndex the {@link PositionListIndex}
     * @deprecated this instance manages the {@link PositionListIndex} itself
     */
    public void setPositionListIndex(PositionListIndex positionListIndex) {
        this.positionListIndexCache = new SoftReference<>(positionListIndex);
        this.positionListIndexLock = null;
    }

    /**
     * Set the {@link PositionListIndex} for this instance explicitly.
     *
     * @param positionListIndex the {@link PositionListIndex}
     */
    public void setAndLockPositionListIndex(PositionListIndex positionListIndex) {
        this.positionListIndexCache = new SoftReference<>(positionListIndex);
        this.positionListIndexLock = positionListIndex;
    }

    @Override
    public BitSet getColumnIndices() {
        return this.columnIndices;
    }

    @Override
    public Relation getRelation() {
        return relation;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        String separator = "";
        sb.append("[");
        for (int index = this.columnIndices.nextSetBit(0);
             index != -1;
             index = this.columnIndices.nextSetBit(index + 1)) {
            sb.append(separator).append(this.relation.getColumns().get(index).getName());
            separator = ", ";
        }
        return sb.append("]").toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnCombination that = (ColumnCombination) o;
        return Objects.equals(columnIndices, that.columnIndices) &&
                Objects.equals(relation, that.relation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnIndices, relation);
    }
}
