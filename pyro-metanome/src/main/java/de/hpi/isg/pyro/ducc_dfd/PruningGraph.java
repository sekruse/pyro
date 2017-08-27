package de.hpi.isg.pyro.ducc_dfd;


import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;

import java.util.*;

/**
 * Index for a cover of {@link Vertical}s. First, sets of {@link Vertical}s are indexed into partitions by their subsets. As more
 * {@link Vertical}s are added, the partitions might be split, i.e., their subset keys are added new elements.
 *
 * @author Patrick Schulze
 * @author Sebastian Kruse
 */
public class PruningGraph {

    /**
     * Characterizes the type of cover that is represented in a {@link PruningGraph}.
     */
    public enum CoverType {

        /**
         * Declares that a {@link PruningGraph} contains a subset cover, i.e., {@link Vertical}s imply superset {@link Vertical}.
         */
        SUBSETS(true),

        /**
         * Declares that a {@link PruningGraph} contains a superset cover, i.e., {@link Vertical}s imply subset {@link Vertical}.
         */
        SUPERSETS(false);

        final boolean isSubsetCover;

        CoverType(boolean isSubsetCover) {
            this.isSubsetCover = isSubsetCover;
        }

        /**
         * @return whether this instance is a subset cover
         */
        public boolean isSubsetCover() {
            return this.isSubsetCover;
        }

        /**
         * @return whether this instance is a superset cover
         */
        public boolean isSupersetCover() {
            return !this.isSubsetCover;
        }
    }

    /**
     * The {@link RelationSchema} of the {@link Vertical}s stored in this instance.
     */
    private final RelationSchema schema;

    /**
     * The maximum number of elements to keep in a partition.
     */
    private final int partitionCapacity;

    /**
     * The {@link CoverType} of this instance.
     */
    private final CoverType coverType;

    /**
     * Contains the partitions.
     */
    private final Map<Vertical, Set<Vertical>> index = new HashMap<>();


    public PruningGraph(RelationSchema schema, CoverType coverType, int overflowTreshold) {
        this.schema = schema;
        this.partitionCapacity = overflowTreshold;
        this.coverType = coverType;
        // Initialize this instance with the most general keys.
        for (Column column : schema.getColumns()) {
            this.index.put(column, new HashSet<>());
        }
    }

    /**
     * Add a {@link Vertical} to this instance, thereby enforcing the {@link CoverType}.
     *
     * @param vertical to be added
     */
    public void add(Vertical vertical) {
        LinkedList<Vertical> fullPartitionKeys = new LinkedList<>();
        Outer:
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey)) {
                Set<Vertical> indexPartition = entry.getValue();
                for (Iterator<Vertical> iterator = indexPartition.iterator(); iterator.hasNext(); ) {
                    Vertical element = iterator.next();
                    if (this.coverType.isSubsetCover() && vertical.contains(element) || this.coverType.isSupersetCover() && element.contains(vertical)) {
                        continue Outer;
                    }
                    if (this.coverType.isSubsetCover() && element.contains(vertical) || this.coverType.isSupersetCover() && vertical.contains(element)) {
                        iterator.remove();
                    }
                }
                indexPartition.add(vertical);

                if (indexPartition.size() > this.partitionCapacity) {
                    fullPartitionKeys.add(partitionKey);
                }
            }
        }

        for (Vertical fullPartitionKey : fullPartitionKeys) {
            this.rebalancePartition(fullPartitionKey);
        }
    }

    /**
     * Discover an arbitrary {@link Vertical} covering the given {@link Vertical}.
     *
     * @param vertical for which a covering {@link Vertical} is queried
     * @return a covering {@link Vertical} or {@code null} if none
     */
    public Vertical getCoverElement(Vertical vertical) {
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey)) {
                for (Vertical coverVertical : entry.getValue()) {
                    if (this.coverType.isSubsetCover() && vertical.contains(coverVertical) || this.coverType.isSupersetCover() && coverVertical.contains(vertical)) {
                        return coverVertical;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Split the partition for the given key.
     *
     * @param partitionKey the key of the partition to be split
     */
    private void rebalancePartition(Vertical partitionKey) {
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
}
