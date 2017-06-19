package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.ColumnIdentifier;

import java.util.BitSet;
import java.util.Objects;
import java.util.Random;

/**
 * Comprises the data of a {@link Column}.
 */
public class ColumnData {

    /**
     * The {@link Column} whose data is being stored by this instance.
     */
    private final Column column;

    private final int[] probingTable;

    private PositionListIndex positionListIndex;

    public ColumnData(Column column, int[] probingTable, PositionListIndex positionListIndex) {
        this.column = column;
        this.probingTable = probingTable;
        this.positionListIndex = positionListIndex;
    }

    public int[] getProbingTable() {
        return this.probingTable;
    }

    public int getProbingTableValue(int tupleIndex) {
        return this.probingTable[tupleIndex];
    }

    public Column getColumn() {
        return column;
    }

    public PositionListIndex getPositionListIndex() {
        return positionListIndex;
    }

    public void shuffle() {
        shuffle(this.probingTable, new Random());
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
    public String toString() {
        return "Data for " + this.column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ColumnData that = (ColumnData) o;
        return Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column);
    }
}
