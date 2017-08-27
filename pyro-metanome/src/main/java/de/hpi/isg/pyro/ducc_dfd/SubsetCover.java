package de.hpi.isg.pyro.ducc_dfd;

import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Index for a subset cover of {@link Vertical}s. That is, there must be no elements that are supersets of existing
 * {@link Vertical}s.
 *
 * @author Patrick Schulze
 * @author Sebastian Kruse
 */
public class SubsetCover extends Cover {

    public SubsetCover(RelationSchema schema, int overflowTreshold) {
        super(schema, overflowTreshold);
    }

    @Override
    public void add(Vertical vertical) {
        if (this.containsSubset(vertical)) return;

        LinkedList<Vertical> fullPartitionKeys = new LinkedList<>();
        boolean isAdded = false;
        boolean isRemoved = false;
        Outer:
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey)) {
                Set<Vertical> indexPartition = entry.getValue();
                // Remove any supersets.
                isRemoved |= indexPartition.removeIf(element -> element.contains(vertical));
                // Add the new vertical.
                indexPartition.add(vertical);
                // Re-partition if necessary.
                if (indexPartition.size() > this.partitionCapacity) {
                    fullPartitionKeys.add(partitionKey);
                }
                isAdded = true;
            }
        }

        if (isRemoved) {
            // If we have removed a superset, it might also be in the other partitions. Therefore, clean them, too.
            for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
                Vertical partitionKey = entry.getKey();
                if (!vertical.contains(partitionKey)) {
                    Set<Vertical> indexPartition = entry.getValue();
                    indexPartition.removeIf(element -> element.contains(vertical));
                }
            }
        }

        if (isAdded) {
            for (Vertical fullPartitionKey : fullPartitionKeys) {
                this.rebalancePartition(fullPartitionKey);
            }
        } else {
            // In the unexpected case that we could not add the vertical, that means that there was no matching partition.
            // In other words, partitions have been re-partitioned so that the keys have become so specific that there is
            // no subset key anymore for the new vertical. In that case, we just add a new partition for the current key
            // and remove all other partitions.
            this.index.keySet().removeIf(key -> key.contains(vertical));
            this.index.put(vertical, Collections.singleton(vertical));
        }
    }

    @Override
    public Vertical getCoverElement(Vertical vertical) {
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey)) {
                for (Vertical coverVertical : entry.getValue()) {
                    if (vertical.contains(coverVertical)) {
                        return coverVertical;
                    }
                }
            }
        }
        return null;
    }

}
