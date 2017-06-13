package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.BitSets;
import de.hpi.isg.pyro.util.PositionListIndex;
import de.hpi.isg.pyro.util.VerticalMap;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Represents a relational table.
 */
public abstract class Relation {

    public static final int singletonValueId = PositionListIndex.singletonValueId;

    public static final int nullValueId = -1;

    protected final String name;

    protected final List<Column> columns;

    private final boolean isNullEqualNull;

    public final Vertical emptyVertical = Vertical.emptyVertical(this);

    // Profiling data.
    public long _loadMillis = 0;
    public AtomicLong _hittingSetNanos = new AtomicLong();
    public AtomicLong _numHittingSets = new AtomicLong();

    protected Relation(String name, boolean isNullEqualNull) {
        this.name = name;
        this.columns = new ArrayList<>();
        this.isNullEqualNull = isNullEqualNull;
    }

    public String getName() {
        return this.name;
    }

    public List<Column> getColumns() {
        return this.columns;
    }

    abstract public int getNumRows();

    public double getMaximumEntropy() {
        return Math.log(this.getNumRows());
    }

    public double getMaximumNip() {
        return this.getNumRows() * (this.getNumRows() - 1d) / 2;
    }

    public long getNumTuplePairs() {
        return this.getNumRows() * (this.getNumRows() - 1L) / 2;
    }

    public ColumnIdentifier getColumnIdentifier(int index) {
        return new ColumnIdentifier(this.getName(), this.columns.get(index).getName());
    }

    public ColumnCombination getColumnCombination(BitSet indices) {
        return this.getColumnCombination(BitSets.toIntArray(indices));
    }


    public ColumnCombination getColumnCombination(int... indices) {
        ColumnIdentifier[] columnIdentifiers = new ColumnIdentifier[indices.length];
        for (int i = 0; i < indices.length; i++) {
            int index = indices[i];
            columnIdentifiers[i] = this.getColumnIdentifier(index);
        }
        return new ColumnCombination(columnIdentifiers);
    }

    public Vertical getVertical(int... indices) {
        if (indices.length == 0) throw new IllegalArgumentException();

        if (indices.length == 1) {
            return this.columns.get(indices[0]);
        }

        BitSet bitSet = new BitSet(this.getNumColumns());
        for (int i = 0; i < indices.length; i++) {
            bitSet.set(i);
        }
        return this.getVertical(bitSet);
    }

    public Vertical getVertical(BitSet indices) {
        if (indices.isEmpty()) return this.emptyVertical;

        if (indices.cardinality() == 1) {
            return this.columns.get(indices.nextSetBit(0));
        }

        return new de.hpi.isg.pyro.model.ColumnCombination(indices, this);
    }

    public void shuffleColumns() {
        throw new UnsupportedOperationException();
    }

    public Column getColumn(String name) {
        for (Column column : this.columns) {
            if (column.getName().equals(name)) {
                return column;
            }
        }
        return null;
    }

    public Column getColumn(int index) {
        return this.columns.get(index);
    }

    public int getNumColumns() {
        return this.columns.size();
    }

    public Relation copy() {
        throw new UnsupportedOperationException();
    }

    abstract public int[] getTuple(int tupleIndex);

    public boolean isNullEqualNull() {
        return this.isNullEqualNull;
    }


    /**
     * Calculate the minimum hitting set for the given {@code verticals}.
     *
     * @param verticals       whose minimum hitting set is requested
     * @param pruningFunction tells whether an intermittent hitting set should be pruned
     * @return the minimum hitting set
     */
    public Collection<Vertical> calculateHittingSet(
            Collection<Vertical> verticals,
            Predicate<Vertical> pruningFunction) {

        long _startNanos = System.nanoTime();

        List<Vertical> sortedVerticals = new ArrayList<>(verticals);
        sortedVerticals.sort(Comparator.comparing(Vertical::getArity).reversed());
        VerticalMap<Vertical> consolidatedVerticals = new VerticalMap<>(this);

        VerticalMap<Vertical> hittingSet = new VerticalMap<>(this);
        hittingSet.put(this.emptyVertical, this.emptyVertical);

        // Now, continuously refine these escaped LHS.
        for (Vertical vertical : sortedVerticals) {
            if (!consolidatedVerticals.getSubsetEntries(vertical).isEmpty()) continue;
            consolidatedVerticals.put(vertical, vertical);

            // All hitting set member that are disjoint from the vertical are invalid.
            ArrayList<Vertical> invalidHittingSetMembers = hittingSet.getSubsetKeys(vertical.invert());
            invalidHittingSetMembers.sort(Comparator.comparing(Vertical::getArity));

            // Remove the invalid hitting set members.
            for (Vertical invalidHittingSetMember : invalidHittingSetMembers) {
                hittingSet.remove(invalidHittingSetMember);
            }

            // Add corrected hitting set members.
            for (Vertical invalidMember : invalidHittingSetMembers) {
                for (int correctiveColumnIndex = vertical.getColumnIndices().nextSetBit(0);
                     correctiveColumnIndex != -1;
                     correctiveColumnIndex = vertical.getColumnIndices().nextSetBit(correctiveColumnIndex + 1)) {

                    Column correctiveColumn = this.getColumn(correctiveColumnIndex);
                    Vertical correctedMember = invalidMember.union(correctiveColumn);

                    // This way, we will never add non-minimal members, because our invalid members are sorted.
                    if (hittingSet.getSubsetEntries(correctedMember).isEmpty()
                            && (pruningFunction == null || !pruningFunction.test(correctedMember))) {
                        hittingSet.put(correctedMember, correctedMember);
                    }
                }
                hittingSet.remove(invalidMember);
            }
        }

        _hittingSetNanos.addAndGet(System.nanoTime() - _startNanos);
        _numHittingSets.incrementAndGet();

        // Produce the result.
        return hittingSet.keySet();
    }
}
