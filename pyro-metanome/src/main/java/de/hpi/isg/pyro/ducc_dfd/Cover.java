package de.hpi.isg.pyro.ducc_dfd;


import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Index for a cover of {@link Vertical}s. First, sets of {@link Vertical}s are indexed into partitions by their subsets. As more
 * {@link Vertical}s are added, the partitions might be split, i.e., their subset keys are added new elements.
 *
 * @author Patrick Schulze
 * @author Sebastian Kruse
 */
abstract public class Cover {

    /**
     * The {@link RelationSchema} of the {@link Vertical}s stored in this instance.
     */
    private final RelationSchema schema;

    /**
     * The maximum number of elements to keep in a partition.
     */
    protected final int partitionCapacity;

    /**
     * Contains the partitions.
     */
    protected final Map<Vertical, Set<Vertical>> index = new HashMap<>();


    protected Cover(RelationSchema schema, int overflowTreshold) {
        this.schema = schema;
        this.partitionCapacity = overflowTreshold;
        // Initialize this instance with the most general keys.
        for (Column column : schema.getColumns()) {
            this.index.put(column, new HashSet<>());
        }
    }

    /**
     * Add a {@link Vertical} to this instance, thereby enforcing the cover.
     *
     * @param vertical to be added
     */
    public abstract void add(Vertical vertical);

    /**
     * Discover an arbitrary {@link Vertical} covering the given {@link Vertical}.
     *
     * @param vertical for which a covering {@link Vertical} is queried
     * @return a covering {@link Vertical} or {@code null} if none
     */
    public abstract Vertical getCoverElement(Vertical vertical);

    /**
     * Discover an arbitrary {@link Vertical} covered by the given {@link Vertical}.
     *
     * @param vertical for which a covered {@link Vertical} is queried
     * @return whether such a subset {@link Vertical} exists
     */
    public boolean containsSubset(Vertical vertical) {
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey)) {
                for (Vertical coverVertical : entry.getValue()) {
                    if (vertical.contains(coverVertical)) return true;
                }
            }
        }
        return false;
    }

    /**
     * Discover an arbitrary {@link Vertical} covering the given {@link Vertical}.
     *
     * @param vertical for which a covering {@link Vertical} is queried
     * @return whether such a superset {@link Vertical} exists
     */
    public boolean containsSuperset(Vertical vertical) {
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey) || partitionKey.contains(vertical)) {
                for (Vertical coverVertical : entry.getValue()) {
                    if (coverVertical.contains(vertical)) return true;
                }
                return false; // We can stop early on because if a superset exists it must be in this partition.
            }
        }
        return false;
    }

    /**
     * Split the partition for the given key.
     *
     * @param partitionKey the key of the partition to be split
     */
    protected void rebalancePartition(Vertical partitionKey) {
        Set<Vertical> partition = this.index.get(partitionKey);

        for (int columnIndex = partitionKey.getColumnIndices().nextClearBit(0);
             columnIndex < this.schema.getNumColumns();
             columnIndex = partitionKey.getColumnIndices().nextClearBit(columnIndex + 1)) {
            Vertical newPartitionKey = partitionKey.union(this.schema.getColumn(columnIndex));

            // Avoid creating a new partition twice.
            if (this.index.containsKey(newPartitionKey)) continue;

            // Create and store the new partition.
            Set<Vertical> newPartition = new HashSet<>();
            for (Vertical vertical : partition) {
                if (vertical.contains(newPartitionKey)) newPartition.add(vertical);
            }
            this.index.put(newPartitionKey, newPartition);
        }
        // Remove the old partition.
        this.index.remove(partitionKey);
    }

    /**
     * Collect all the {@link Vertical}s in this cover in a {@link Set}.
     * @return the {@link Set}
     */
    public Set<Vertical> toSet() {
        return this.index.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return "Cover" + this.toSet();
    }
}
