package de.hpi.isg.pyro.util;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;

import java.io.Serializable;
import java.util.*;
import java.util.function.BiConsumer;

/**
 * This class uses a trie to map {@link Vertical}s to values.
 */
@DefaultSerializer(FieldSerializer.class)
public class VerticalMap<Value> implements Map<Vertical, Value>, Serializable {

    private final RelationSchema relation;

    private final SetTrie<Value> setTrie;

    private int size = 0;

    public VerticalMap(RelationSchema relation) {
        this.relation = relation;
        this.setTrie = new SetTrie<>(this.relation.getNumColumns());
    }

    @Override
    public int size() {
        return this.size;
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return this.get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value get(Object key) {
        if (key instanceof Vertical) {
            return this.setTrie.get(((Vertical) key).getColumnIndices(), 0);
        } else if (key instanceof BitSet) {
            return this.setTrie.get((BitSet) key, 0);
        }
        throw new IllegalArgumentException("Illegal key for a VerticalMap.");
    }

    @Override
    public Value put(Vertical key, Value value) {
        Value oldValue = this.setTrie.associate(key.getColumnIndices(), 0, value);
        if (oldValue == null) this.size++;
        return oldValue;
    }

    @Override
    public Value remove(Object key) {
        Value removedValue;
        if (key instanceof Vertical) {
            removedValue = this.setTrie.remove(((Vertical) key).getColumnIndices(), 0);
            return removedValue;
        } else if (key instanceof BitSet) {
            removedValue = this.setTrie.remove((BitSet) key, 0);
        } else {
            throw new IllegalArgumentException("Illegal key for a VerticalMap.");
        }
        if (removedValue != null) this.size--;
        return removedValue;
    }

    @Override
    public void putAll(Map<? extends Vertical, ? extends Value> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    public ArrayList<Vertical> getSubsetKeys(Vertical vertical) {
        ArrayList<Vertical> subsetKeys = new ArrayList<>();
        this.setTrie.collectSubsetKeys(
                vertical.getColumnIndices(),
                0,
                new BitSet(this.relation.getNumColumns()),
                (k, v) -> subsetKeys.add(this.relation.getVertical(k))
        );
        return subsetKeys;
    }

    public ArrayList<Entry<Vertical, Value>> getSubsetEntries(Vertical vertical) {
        ArrayList<Entry<Vertical, Value>> entries = new ArrayList<>();
        this.setTrie.collectSubsetKeys(
                vertical.getColumnIndices(),
                0,
                new BitSet(this.relation.getNumColumns()),
                (k, v) -> entries.add(new VerticalEntry<>(this.relation.getVertical(k), v))
        );
        return entries;
    }

    public ArrayList<Entry<Vertical, Value>> getSupersetEntries(Vertical vertical) {
        ArrayList<Entry<Vertical, Value>> entries = new ArrayList<>();
        this.setTrie.collectSupersetKeys(
                vertical.getColumnIndices(),
                0,
                new BitSet(this.relation.getNumColumns()),
                (k, v) -> entries.add(new VerticalEntry<>(this.relation.getVertical(k), v))
        );
        return entries;
    }


    public ArrayList<Entry<Vertical, Value>> getRestrictedSupersetEntries(Vertical vertical, Vertical exclusion) {
        if (vertical.getColumnIndices().intersects(exclusion.getColumnIndices())) {
            throw new IllegalArgumentException();
        }

        ArrayList<Entry<Vertical, Value>> entries = new ArrayList<>();
        this.setTrie.collectRestrictedSupersetKeys(
                vertical.getColumnIndices(),
                exclusion.getColumnIndices(),
                0,
                new BitSet(this.relation.getNumColumns()),
                (k, v) -> entries.add(new VerticalEntry<>(this.relation.getVertical(k), v))
        );
        return entries;
    }

    public boolean removeSupersetEntries(Vertical key) {
        ArrayList<Entry<Vertical, Value>> supersetEntries = this.getSupersetEntries(key);
        for (Entry<Vertical, Value> supersetEntry : supersetEntries) {
            this.remove(supersetEntry.getKey());
        }
        return !supersetEntries.isEmpty();
    }


    public boolean removeSubsetEntries(Vertical key) {
        ArrayList<Entry<Vertical, Value>> subsetEntries = this.getSubsetEntries(key);
        for (Entry<Vertical, Value> subsetEntry : subsetEntries) {
            this.remove(subsetEntry.getKey());
        }
        return !subsetEntries.isEmpty();
    }

    @Override
    public Set<Vertical> keySet() {
        Set<Vertical> keySet = new HashSet<>();
        this.setTrie.traverseEntries(
                new BitSet(this.relation.getNumColumns()),
                (k, v) -> keySet.add(this.relation.getVertical(k))
        );
        return keySet;
    }

    @Override
    public Collection<Value> values() {
        Collection<Value> values = new ArrayList<>();
        this.setTrie.traverseEntries(
                new BitSet(this.relation.getNumColumns()),
                (k, v) -> values.add(v)
        );
        return values;
    }

    @Override
    public Set<Entry<Vertical, Value>> entrySet() {
        Set<Entry<Vertical, Value>> entrySet = new HashSet<>();
        this.setTrie.traverseEntries(
                new BitSet(this.relation.getNumColumns()),
                (k, v) -> entrySet.add(new VerticalEntry<>(this.relation.getVertical(k), v))
        );
        return entrySet;
    }

    private static final class SetTrie<Value> implements Serializable {

        private final int offset, dimension;

        /**
         * This array is lazy-initialized.
         */
        private SetTrie[] subtries;

        protected Value value = null;

        /**
         * Creates a new {@link SetTrie} that is able to store sets with the given alphabet size.
         *
         * @param dimension the size of the alphabet of this graph
         */
        public SetTrie(int dimension) {
            this(0, dimension);
        }

        /**
         * Creates a new instance. Should be used for nested instances.
         */
        private SetTrie(int offset, int dimension) {
            this.offset = offset;
            this.dimension = dimension;
        }

        /**
         * Adds a column combination to the graph. Returns the graph after adding.
         *
         * @param key a column combination to add
         * @return the graph
         */
        public Value associate(BitSet key, int nextBit, Value value) {
            nextBit = key.nextSetBit(nextBit);
            if (nextBit == -1) {
                Value oldValue = this.value;
                this.value = value;
                return oldValue;
            }

            return this.getOrCreateSubTrie(nextBit).associate(key, nextBit + 1, value);
        }

        /**
         * Retrieves the value associated with a certain key.
         *
         * @param key     a {@link BitSet}, which serves as key
         * @param nextBit the next bit to consider (or any unset bit before this bit)
         * @return the associated value or {@code null} if none
         */
        public Value get(BitSet key, int nextBit) {
            nextBit = key.nextSetBit(nextBit);
            if (nextBit == -1) {
                return this.value;
            }

            SetTrie<Value> subtrie = this.subtrie(nextBit);
            if (subtrie == null) return null;
            return subtrie.get(key, nextBit + 1);
        }

        /**
         * Removes a column combination from the graph. Returns, whether an element was removed.
         *
         * @param key     a column combination to remove
         * @param nextBit the next bit to consider (or any unset bit before this bit)
         * @return the removed value or {@code null} if none
         */
        public Value remove(BitSet key, int nextBit) {
            nextBit = key.nextSetBit(nextBit);
            if (nextBit == -1) {
                Value removedValue = this.value;
                this.value = null;
                return removedValue;
            }

            SetTrie<Value> subtrie = this.subtrie(nextBit);
            if (subtrie == null) return null;
            Value removedValue = subtrie.remove(key, nextBit + 1);
            if (subtrie.isEmpty()) {
                this.subtries[nextBit - this.offset] = null;
            }

            return removedValue;
        }

        /**
         * Detect whether this instance is empty.
         *
         * @return whether this instance neither has a value nor sub-instances
         */
        private boolean isEmpty() {
            if (this.value != null) return false;
            if (this.subtries == null) return true;
            for (SetTrie subtrie : this.subtries) {
                if (subtrie != null) return false;
            }
            return true;
        }

        /**
         * Looks for the subgraph or builds and adds a new one.
         *
         * @param index the column index to perform the lookup on
         * @return the subgraph behind the column index
         */
        @SuppressWarnings("unchecked")
        protected SetTrie<Value> getOrCreateSubTrie(int index) {
            if (this.subtries == null) this.subtries = new SetTrie[this.dimension - this.offset];
            SetTrie subtrie = this.subtrie(index);
            if (subtrie == null) {
                subtrie = new SetTrie(index + 1, this.dimension);
                subtries[index - this.offset] = subtrie;
            }
            return subtrie;
        }

        /**
         * Looks for a subtrie with the given index.
         *
         * @param index the column index to perform the lookup on
         * @return the subgraph behind the column index
         */
        @SuppressWarnings("unchecked")
        protected SetTrie<Value> subtrie(int index) {
            if (this.subtries == null) return null;
            if (index < this.offset || index >= this.dimension) {
                throw new IllegalArgumentException("Illegal subtrie index encountered: " + index);
            }
            return this.subtries[index - this.offset];
        }

        private void traverseEntries(BitSet subsetKey, BiConsumer<BitSet, Value> collector) {
            if (this.value != null) {
                collector.accept((BitSet) subsetKey.clone(), this.value);
            }

            for (int i = this.offset; i < this.dimension; i++) {
                SetTrie<Value> subtrie = this.subtrie(i);
                if (subtrie != null) {
                    subsetKey.set(i);
                    subtrie.traverseEntries(subsetKey, collector);
                    subsetKey.clear(i);
                }
            }
        }

        /**
         * Returns all subsets of the given column combination that are in the graph.
         *
         * @param key       a {@link BitSet}, which serves as key
         * @param nextBit   the next bit to consider (or any unset bit before this bit)
         * @param subsetKey keeps track of the already matched bits from the {@code key}
         * @param collector collects all the subset keys
         */
        public void collectSubsetKeys(BitSet key, int nextBit, BitSet subsetKey, BiConsumer<BitSet, Value> collector) {
            nextBit = key.nextSetBit(nextBit);
            while (nextBit != -1) {
                SetTrie<Value> subtrie = this.subtrie(nextBit);
                if (subtrie != null) {
                    subsetKey.set(nextBit);
                    subtrie.collectSubsetKeys(key, nextBit + 1, subsetKey, collector);
                    subsetKey.clear(nextBit);
                }
                nextBit = key.nextSetBit(nextBit + 1);
            }

            if (this.value != null) {
                collector.accept((BitSet) subsetKey.clone(), this.value);
            }
        }

        /**
         * Returns all supersets of the given column combination that are in the graph.
         *
         * @param key         a {@link BitSet}, which serves as key
         * @param nextBit     the next bit to consider
         * @param supersetKey keeps track of the already used columns
         * @param collector   collects all the subset keys
         */
        public void collectSupersetKeys(BitSet key, int nextBit, BitSet supersetKey, BiConsumer<BitSet, Value> collector) {
            nextBit = nextBit == -1 ? nextBit : key.nextSetBit(nextBit);
            if (nextBit == -1) {
                // In this case, we already have a superset of the key. We have to do full traversal.
                if (this.value != null) {
                    collector.accept((BitSet) supersetKey.clone(), this.value);
                }
                for (int i = this.offset; i < this.dimension; i++) {
                    SetTrie<Value> subtrie = this.subtrie(i);
                    if (subtrie != null) {
                        supersetKey.set(i);
                        subtrie.collectSupersetKeys(key, nextBit, supersetKey, collector);
                        supersetKey.clear(i);
                    }
                }
            } else {
                // Otherwise, we have to do two things:
                // (1) Do not select the next bit, but dive into subtries *before* this bit. This corresponds to
                // adding an intermittent extra bit w.r.t. the input key.
                for (int i = this.offset; i < nextBit; i++) {
                    SetTrie<Value> subtrie = this.subtrie(i);
                    if (subtrie != null) {
                        supersetKey.set(i);
                        subtrie.collectSupersetKeys(key, nextBit, supersetKey, collector);
                        supersetKey.clear(i);
                    }
                }

                // (2) Select the bit and dive into the corresponding subtrie. This corresponds to selecting a bit
                // from the input key.
                SetTrie<Value> subtrie = this.subtrie(nextBit);
                if (subtrie != null) {
                    supersetKey.set(nextBit);
                    subtrie.collectSupersetKeys(key, nextBit + 1, supersetKey, collector);
                    supersetKey.clear(nextBit);
                }
            }
        }

        /**
         * Returns all restricted (i.e., with some bits excluded) supersets of the given column combination that are in the graph.
         *
         * @param key         a {@link BitSet}, which serves as key
         * @param blacklist   defines which bits must not be selected in the superset
         * @param nextBit     the next bit to consider
         * @param supersetKey keeps track of the already used columns
         * @param collector   collects all the subset keys
         */
        public void collectRestrictedSupersetKeys(BitSet key, BitSet blacklist, int nextBit, BitSet supersetKey, BiConsumer<BitSet, Value> collector) {
            nextBit = nextBit == -1 ? nextBit : key.nextSetBit(nextBit);
            if (nextBit == -1) {
                // In this case, we already have a superset of the key. We have to do full traversal.
                if (this.value != null) {
                    collector.accept((BitSet) supersetKey.clone(), this.value);
                }
                for (int i = this.offset; i < this.dimension; i++) {
                    if (blacklist.get(i)) continue;
                    SetTrie<Value> subtrie = this.subtrie(i);
                    if (subtrie != null) {
                        supersetKey.set(i);
                        subtrie.collectRestrictedSupersetKeys(key, blacklist, nextBit, supersetKey, collector);
                        supersetKey.clear(i);
                    }
                }
            } else {
                // Otherwise, we have to do two things:
                // (1) Do not select the next bit, but dive into subtries *before* this bit. This corresponds to
                // adding an intermittent extra bit w.r.t. the input key. However, we must check against the blacklist.
                for (int i = this.offset; i < nextBit; i++) {
                    if (blacklist.get(i)) continue;
                    SetTrie<Value> subtrie = this.subtrie(i);
                    if (subtrie != null) {
                        supersetKey.set(i);
                        subtrie.collectRestrictedSupersetKeys(key, blacklist, nextBit, supersetKey, collector);
                        supersetKey.clear(i);
                    }
                }

                // (2) Select the bit and dive into the corresponding subtrie. This corresponds to selecting a bit
                // from the input key.
                SetTrie<Value> subtrie = this.subtrie(nextBit);
                if (subtrie != null) {
                    supersetKey.set(nextBit);
                    subtrie.collectRestrictedSupersetKeys(key, blacklist, nextBit + 1, supersetKey, collector);
                    supersetKey.clear(nextBit);
                }
            }
        }

//
//        /**
//         * Returns whether at least one subset is contained in the graph. The method returns when the
//         * first subset is found in the graph. This is possibly faster than
//         * {@link SetTrie#getExistingSubsets(ColumnCombinationBitset)}, because a smaller part of the
//         * graph needs be traversed.
//         *
//         * @param superset the set for which the graph should be checked for subsets
//         * @return whether at least one subset is contained in the graph
//         */
//        public boolean containsSubsetKey(ColumnCombinationBitset superset) {
//            if (this.isEmpty()) {
//                return false;
//            }
//            Queue<SearchTask> openTasks = new LinkedList<>();
//            openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));
//
//            while (!openTasks.isEmpty()) {
//                SearchTask currentTask = openTasks.remove();
//                // If the current subgraph is empty a subset has been found
//                if (currentTask.subGraph.isEmpty()) {
//                    return true;
//                }
//
//                if (currentTask.subGraph.subSetEnds) {
//                    return true;
//                }
//
//                // Iterate over the remaining column indices
//                for (int i = currentTask.numberOfCheckedColumns; i < superset.size(); i++) {
//                    int currentColumnIndex = superset.getSetBits().get(i);
//                    // Get the subgraph behind the current index
//                    SetTrie subGraph =
//                            currentTask.subGraph.subGraphs[currentColumnIndex];
//                    // column index is not set on any set --> check next column index
//                    if (subGraph != null) {
//                        // Add the current column index to the path
//                        ColumnCombinationBitset path =
//                                new ColumnCombinationBitset(currentTask.path)
//                                        .addColumn(currentColumnIndex);
//
//                        openTasks.add(new SearchTask(subGraph, i + 1, path));
//                    }
//                }
//            }
//
//            return false;
//        }
//
//        /**
//         * The method returns all minimal subsets contained in the graph using a breadth-first search
//         * pattern. Non minimal subsets are not traversed.
//         *
//         * @return a list containing all minimal subsets
//         */
//        public Set<ColumnCombinationBitset> getMinimalSubsets() throws ColumnIndexOutOfBoundsException {
//            if (this.isEmpty()) {
//                return new TreeSet<>();
//            }
//
//            SetTrie graph = new SetTrie(this.subtries.length);
//            TreeSet<ColumnCombinationBitset> result = new TreeSet<>();
//            TreeSet<SearchTask> openTasks = new TreeSet<>();
//            openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));
//
//            while (!openTasks.isEmpty()) {
//                SearchTask currentTask = openTasks.pollFirst();
//                if (currentTask.subGraph.subSetEnds) {
//                    if (!graph.containsSubset(currentTask.path)) {
//                        graph.add(currentTask.path);
//                        result.add(currentTask.path);
//                    }
//                } else {
//                    // Iterate over the remaining column indices
//                    for (int columnIndex = 0; columnIndex < currentTask.subGraph.subGraphs.length;
//                         columnIndex++) {
//                        // Get the subgraph behind the current index
//                        SetTrie subGraph =
//                                currentTask.subGraph.subGraphs[columnIndex];
//                        // column index is not set on any set --> check next column index
//                        if (subGraph != null) {
//                            // Add the current column index to the path
//                            ColumnCombinationBitset path =
//                                    new ColumnCombinationBitset(currentTask.path)
//                                            .addColumn(columnIndex);
//
//                            openTasks
//                                    .add(new SearchTask(subGraph, columnIndex + 1, path));
//                        }
//                    }
//                }
//            }
//            return result;
//        }
//
//        /**
//         * Returns all supersets of the given column combination that are in the graph.
//         *
//         * @param subset given subset to search for supersets
//         * @return a list containing all found supersets
//         */
//        public ArrayList<ColumnCombinationBitset> getExistingSupersets(ColumnCombinationBitset subset) {
//            ArrayList<ColumnCombinationBitset> supersets = new ArrayList<>();
//
//            if (this.isEmpty()) {
//                return supersets;
//            }
//
//            Queue<SearchTask> openTasks = new LinkedList<>();
//            openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));
//
//            while (!openTasks.isEmpty()) {
//                SearchTask currentTask = openTasks.remove();
//
//                List<Integer> setBits = subset.getSetBits();
//                if (setBits.size() <= currentTask.numberOfCheckedColumns) {
//                    supersets.addAll(currentTask.subGraph.getContainedSets(currentTask.path));
//                    continue;
//                }
//                int from;
//                if (currentTask.numberOfCheckedColumns == 0) {
//                    from = 0;
//                } else {
//                    from = setBits.get(currentTask.numberOfCheckedColumns - 1);
//                }
//
//                int upto = setBits.get(currentTask.numberOfCheckedColumns);
//
//                for (int columnIndex = from; columnIndex <= upto; columnIndex++) {
//                    SetTrie subGraph = currentTask.subGraph.subGraphs[columnIndex];
//                    if (subGraph == null) {
//                        continue;
//                    }
//
//                    if (columnIndex == setBits.get(currentTask.numberOfCheckedColumns)) {
//                        openTasks.add(new SearchTask(
//                                subGraph,
//                                currentTask.numberOfCheckedColumns + 1,
//                                new ColumnCombinationBitset(currentTask.path).addColumn(columnIndex)));
//                    } else {
//                        openTasks.add(new SearchTask(
//                                subGraph,
//                                currentTask.numberOfCheckedColumns,
//                                new ColumnCombinationBitset(currentTask.path).addColumn(columnIndex)));
//                    }
//                }
//            }
//
//            return supersets;
//        }
//
//        /**
//         * Returns whether at least one superset is contained in the graph.The method returns when the
//         * first superset is found in the graph. This is possibly faster than
//         * {@link SetTrie#getExistingSupersets(ColumnCombinationBitset)}, because a smaller part of
//         * the graph needs to be traversed.
//         *
//         * @param subset the set for which the graph should be checked for supersets
//         * @return whether at least one superset is contained in the graph
//         */
//        public boolean containsSuperset(ColumnCombinationBitset subset) {
//            if (this.isEmpty()) {
//                return false;
//            }
//
//            Queue<SearchTask> openTasks = new LinkedList<>();
//            openTasks.add(new SearchTask(this, 0, new ColumnCombinationBitset()));
//
//            while (!openTasks.isEmpty()) {
//                SearchTask currentTask = openTasks.remove();
//
//                List<Integer> setBits = subset.getSetBits();
//                if (setBits.size() <= currentTask.numberOfCheckedColumns) {
//                    return true;
//                }
//                int from;
//                if (currentTask.numberOfCheckedColumns == 0) {
//                    from = 0;
//                } else {
//                    from = setBits.get(currentTask.numberOfCheckedColumns - 1);
//                }
//
//                // Only column identifiers coming after the current identifier are relevant, or all remaining.
//                int upto = Math.min(setBits.get(currentTask.numberOfCheckedColumns) + 1, subtries.length);
//
//                for (int columnIndex = from; columnIndex < upto; columnIndex++) {
//                    SetTrie subGraph = currentTask.subGraph.subGraphs[columnIndex];
//                    if (subGraph == null) {
//                        continue;
//                    }
//
//                    if (columnIndex == setBits.get(currentTask.numberOfCheckedColumns)) {
//                        openTasks.add(new SearchTask(
//                                subGraph,
//                                currentTask.numberOfCheckedColumns + 1,
//                                currentTask.path.addColumn(columnIndex)));
//                    } else {
//                        openTasks.add(new SearchTask(
//                                subGraph,
//                                currentTask.numberOfCheckedColumns,
//                                currentTask.path.addColumn(columnIndex)));
//                    }
//                }
//            }
//
//            return false;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) {
//                return true;
//            }
//            if (o == null || getClass() != o.getClass()) {
//                return false;
//            }
//
//            SetTrie that = (SetTrie) o;
//
//            if (subSetEnds != that.subSetEnds) {
//                return false;
//            }
//            // Probably incorrect - comparing Object[] arrays with Arrays.equals
//            return Arrays.equals(subtries, that.subtries);
//
//        }
//
//        @Override
//        public int hashCode() {
//            int result = subtries != null ? Arrays.hashCode(subtries) : 0;
//            result = 31 * result + (subSetEnds ? 1 : 0);
//            return result;
//        }
//
//        @Override
//        public String toString() {
//            List<String> rows = new ArrayList<>();
//
//            stringRepresentation(rows, 0, 0);
//
//            return Joiner.on('\n').join(rows.subList(0, rows.size() - 1));
//        }
//
//        /**
//         * Recursive generation of a string representation of the graph.
//         *
//         * @param rows       the rows of the representation
//         * @param level      the current row level to write to
//         * @param leftMargin how many spaces to leave at the left
//         * @return the number of columns written to in the current row
//         */
//        protected int stringRepresentation(List<String> rows, int level, int leftMargin) {
//            int numberOfColumnsWritten;
//            if (level >= rows.size()) {
//                rows.add("");
//            }
//            StringBuilder row = new StringBuilder(rows.get(level));
//            for (int columnIndex = 0; columnIndex < subtries.length; columnIndex++) {
//                SetTrie subGraph = subtries[columnIndex];
//                if (subGraph == null) {
//                    continue;
//                }
//                while (row.length() < leftMargin) {
//                    row.append(" ");
//                }
//                int newLeftMargin = row.length();
//                row.append(columnIndex);
//                if (subGraph.subSetEnds) {
//                    row.append("X");
//                }
//                row.append(" ");
//                numberOfColumnsWritten = subGraph.stringRepresentation(rows, level + 1, newLeftMargin);
//                while (row.length() < numberOfColumnsWritten) {
//                    row.append(" ");
//                }
//            }
//            rows.set(level, CharMatcher.WHITESPACE.trimTrailingFrom(row));
//
//            return row.length();
//        }

    }

    public static class VerticalEntry<Value> implements Map.Entry<Vertical, Value> {

        private final Vertical key;

        private final Value value;

        public VerticalEntry(Vertical key, Value value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Vertical getKey() {
            return this.key;
        }

        @Override
        public Value getValue() {
            return this.value;
        }

        @Override
        public Value setValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return String.format("(%s, %s)", this.key, this.value);
        }
    }
}
