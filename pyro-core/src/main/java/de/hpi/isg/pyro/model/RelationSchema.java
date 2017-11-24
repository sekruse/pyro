package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.core.ProfilingContext;
import de.hpi.isg.pyro.util.BitSets;
import de.hpi.isg.pyro.util.VerticalMap;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.function.Predicate;

/**
 * Represents the schema of a relational table.
 *
 * @see RelationData
 */
public class RelationSchema implements Serializable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected final String name;

    protected final List<Column> columns;

    private final boolean isNullEqualNull;

    public final Vertical emptyVertical = Vertical.emptyVertical(this);

    public RelationSchema(String name, boolean isNullEqualNull) {
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
            bitSet.set(indices[i]);
        }
        return this.getVertical(bitSet);
    }

    public Vertical getVertical(List<Integer> indices) {
        if (indices.isEmpty()) return emptyVertical;

        if (indices.size() == 1) {
            return this.columns.get(indices.get(0));
        }

        BitSet bitSet = new BitSet(this.getNumColumns());
        for (Integer index : indices) {
            bitSet.set(index);
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

    /**
     * Append a {@link de.hpi.isg.mdms.model.targets.Column} to this instance.
     *
     * @param name the name of the new {@link de.hpi.isg.mdms.model.targets.Column}
     */
    public void appendColumn(String name) {
        this.columns.add(new Column(this, name, this.columns.size()));
    }

    public Column getColumn(int index) {
        return this.columns.get(index);
    }

    public int getNumColumns() {
        return this.columns.size();
    }

    public RelationSchema copy() {
        throw new UnsupportedOperationException();
    }

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
            Predicate<Vertical> pruningFunction,
            ProfilingContext.ProfilingData profilingData) {

        long _startNanos = System.nanoTime();
        int _intermediateHittingSets = 0;

        // Sort verticals by arity to make use of subset relationships.
        List<Vertical> sortedVerticals = new ArrayList<>(verticals);
        sortedVerticals.sort(Comparator.comparing(Vertical::getArity));
        VerticalMap<Vertical> consolidatedVerticals = new VerticalMap<>(this);

        VerticalMap<Vertical> hittingSet = new VerticalMap<>(this);
        hittingSet.put(this.emptyVertical, this.emptyVertical);

        long _prepareNanos = System.nanoTime() - _startNanos;
        long _pruningNanos = 0L;

        // Now, continuously refine these escaped LHS.
        for (Vertical vertical : sortedVerticals) {

            // We can skip any vertical whose supersets we already operated on.
            if (consolidatedVerticals.getAnySubsetEntry(vertical) != null) {
                continue;
            }
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
                    if (hittingSet.getAnySubsetEntry(correctedMember) == null) {
                        _intermediateHittingSets++;
                        if (pruningFunction != null) {
                            long _pruningStartNanos = System.nanoTime();
                            final boolean isPruned = pruningFunction.test(correctedMember);
                            _pruningNanos += System.nanoTime() - _pruningStartNanos;
                            if (isPruned) continue;
                        }
                        hittingSet.put(correctedMember, correctedMember);
                    }
                }
            }

            if (hittingSet.isEmpty()) break;
        }

        long _elapsedNanos = System.nanoTime() - _startNanos;
        long _updateNanos = _elapsedNanos - _prepareNanos;
        profilingData.hittingSetNanos.addAndGet(_elapsedNanos);
        profilingData.hittingSetPruningNanos.addAndGet(_pruningNanos);
        profilingData.numHittingSets.incrementAndGet();

        // Warn if a hitting set calculation took very long.
        if (_elapsedNanos > 1e10) { // 1000 ms
            if (this.logger.isWarnEnabled()) {
                StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
                this.logger.warn(String.format(
                        "Hitting set calculation with %,d (%,d) input and %,d output verticals took %,d ms (called by %s):\n" +
                                "* Preparation:             %,5d (%,.01f%%) ms\n" +
                                "* Evolve solutions:        %,5d (%,.01f%%) ms\n" +
                                "* Test for pruning:        %,5d (%,.01f%%) ms\n" +
                                "* Intermediate solutions:  %,5d #",
                        verticals.size(),
                        consolidatedVerticals.size(),
                        hittingSet.size(),
                        _elapsedNanos / 1_000_000L,
                        stackTrace[2],
                        _prepareNanos / 1_000_000L, _prepareNanos * 100d / _elapsedNanos,
                        _updateNanos / 1_000_000L, _updateNanos * 100d / _elapsedNanos,
                        _pruningNanos / 1_000_000L, _pruningNanos * 100d / _elapsedNanos,
                        _intermediateHittingSets
                ));
            }
        }

        // Produce the result.
        return hittingSet.keySet();
    }
}
