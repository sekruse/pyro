package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.PositionListIndex;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the data of a relation.
 *
 * @see RelationSchema
 */
public abstract class RelationData {

    public static final int singletonValueId = PositionListIndex.singletonValueId;

    public static final int nullValueId = -1;

    private final RelationSchema schema;

    // Profiling data.
    public long _loadMillis = 0;
    public AtomicLong _hittingSetNanos = new AtomicLong();
    public AtomicLong _numHittingSets = new AtomicLong();

    protected RelationData(RelationSchema schema) {
        this.schema = schema;
    }

    abstract public int getNumRows();

    public int getNumColumns() {
        return this.schema.getNumColumns();
    }

    public double getMaximumEntropy() {
        return Math.log(this.getNumRows());
    }

    public double getMaximumNip() {
        return this.getNumRows() * (this.getNumRows() - 1d) / 2;
    }

    public long getNumTuplePairs() {
        return this.getNumRows() * (this.getNumRows() - 1L) / 2;
    }

    public void shuffleColumns() {
        throw new UnsupportedOperationException();
    }

    public RelationData copy() {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves the {@link ColumnData} for the {@link Column}s in the {@link #getSchema() schema} - in the exact same order.
     *
     * @return an array of {@link ColumnData}
     */
    abstract public ColumnData[] getColumnData();

    /**
     * Retrieves the {@link ColumnData} for the {@link Column} with the given index.
     *
     * @param columnIndex the index of the {@link Column}
     * @return the {@link ColumnData}
     */
    abstract public ColumnData getColumnData(int columnIndex);

    abstract public int[] getTuple(int tupleIndex);

    public RelationSchema getSchema() {
        return this.schema;
    }
}
