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
public class SupersetCover extends Cover {

    public SupersetCover(RelationSchema schema, int overflowTreshold) {
        super(schema, overflowTreshold);
    }

    @Override
    public void add(Vertical vertical) {
        if (this.containsSuperset(vertical)) return;

        LinkedList<Vertical> fullPartitionKeys = new LinkedList<>();
        boolean isAdded = false;
        Outer:
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey)) {
                Set<Vertical> indexPartition = entry.getValue();
                // Remove any supersets.
                indexPartition.removeIf(vertical::contains);
                // Add the new vertical.
                indexPartition.add(vertical);
                // Re-partition if necessary.
                if (indexPartition.size() > this.partitionCapacity) {
                    fullPartitionKeys.add(partitionKey);
                }
            }
        }

        if (isAdded) {
            for (Vertical fullPartitionKey : fullPartitionKeys) {
                this.rebalancePartition(fullPartitionKey);
            }
        }
    }

    @Override
    public Vertical getCoverElement(Vertical vertical) {
        for (Map.Entry<Vertical, Set<Vertical>> entry : this.index.entrySet()) {
            Vertical partitionKey = entry.getKey();
            if (vertical.contains(partitionKey) || partitionKey.contains(vertical)) {
                for (Vertical coverVertical : entry.getValue()) {
                    if (coverVertical.contains(vertical)) {
                        return coverVertical;
                    }
                }
                return null;
            }
        }
        return null;
    }

}
