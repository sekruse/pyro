package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.ColumnIdentifier;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.BitSet;
import java.util.Objects;
import java.util.Random;

/**
 * Represents a column in a {@link Relation}.
 */
public class Column implements Vertical {

    private final Relation relation;

    private final String name;

    private final int index;

    private final BitSet indices;

    private final int[] data;

    private PositionListIndex positionListIndex;

    Column(Relation relation, String name, int index, int[] data) {
        this.relation = relation;
        this.name = name;
        this.index = index;
        this.indices = new BitSet(this.index);
        this.indices.set(this.index);
        this.data = data;
    }

    Column(Relation relation, String name, int index, PositionListIndex pli) {
        this(relation, name, index, pli.getProbingTable());
        this.positionListIndex = pli;
    }

    public int[] getData() {
        return data;
    }

    public PositionListIndex getPositionListIndex() {
        if (this.positionListIndex == null) {
            this.positionListIndex = PositionListIndex.createFor(this.data, this.relation.isNullEqualNull());
        }
        return this.positionListIndex;
    }

    public int getIndex() {
        return this.index;
    }

    @Override
    public BitSet getColumnIndices() {
        return this.indices;
    }

    public String getName() {
        return this.name;
    }

    public void shuffle() {
        shuffle(this.data, new Random());
        this.positionListIndex = null;
    }

    public static void shuffle(int[] data, Random random) {
        for (int i = 1; i < data.length; i++) {
            int k = random.nextInt(i);
            swapElements(data, i, k);
        }
    }

    private static void swapElements(int[] data, int index1, int index2) {
        data[index1] ^= data[index2];
        data[index2] ^= data[index1];
        data[index1] ^= data[index2];
    }

    @Override
    public Relation getRelation() {
        return this.relation;
    }

    @Override
    public String toString() {
        return '[' + this.name + ']';
    }

    /**
     * Convert this instance into a {@link ColumnIdentifier}.
     *
     * @return the converted instance
     */
    public ColumnIdentifier toMetanomeColumnIdentifier() {
        return new ColumnIdentifier(this.getRelation().getName(), this.getName());
    }

    public Column copyFor(Relation relationCopy) {
        return new Column(
                relationCopy,
                this.name,
                this.index,
                this.data.clone()
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return index == column.index &&
                Objects.equals(relation, column.relation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relation, index);
    }
}
