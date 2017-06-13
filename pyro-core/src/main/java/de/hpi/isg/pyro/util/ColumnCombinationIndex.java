package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnCombination;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Index that maps pairs of {@link ColumnCombination}s to some values.
 */
public class ColumnCombinationIndex<V> {

    private final Map<BitSet, V> index = new HashMap<>();

    private final Relation relation;

    public ColumnCombinationIndex(Relation relation) {
        this.relation = relation;
    }

    public V put(Vertical key, V value) {
        return this.put(key.getColumnIndices(), value);
    }

    public V put(BitSet key, V value) {
        return this.index.put(key, value);
    }

    public V computeIfAbsent(Vertical key, Supplier<V> supplier) {
        return this.index.computeIfAbsent(key.getColumnIndices(), k -> supplier.get());
    }

    public V get(Column... columns) {
        return this.get(this.toBitSet(columns));
    }

    private BitSet toBitSet(Column[] columns) {
        BitSet bitSet = new BitSet();
        for (Column column : columns) {
            bitSet.set(column.getIndex());
        }
        return bitSet;
    }

    public V get(Vertical key) {
        return this.get(key.getColumnIndices());
    }

    public V get(BitSet key) {
        return this.index.get(key);
    }

    public Map<BitSet, V> getIndex() {
        return this.index;
    }

}
