package de.hpi.isg.pyro.ducc_dfd;

import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.PositionListIndex;

import java.util.function.BiConsumer;

/**
 * UCC graph traversal implementation.
 *
 * @author Jens Ehrlich
 * @author Jakob Zwiener
 * @author Mandy Roick
 * @author Lukas Schulze
 * @author Sebastian Kruse
 */
public class UccGraphTraverser extends GraphTraverser {

    /**
     * The maximum error for AUCCs.
     */
    protected final double maxError;

    /**
     * The number of tuple pairs in the relation.
     */
    protected final long numTuplePairs;

    /**
     * Creates a new instance.
     *
     * @param schema                        which should be traversed
     * @param pliRepository                 provides {@link PositionListIndex}es
     * @param prunedColumns                 that should not be traversed
     * @param pruningGraphPartitionCapacity see {@link Cover#partitionCapacity}
     * @param maxError                      maximum error for candidates to classify as (partial) UCCs
     * @param numTuplePairs                 the number of tuple pairs in the relation
     */
    public UccGraphTraverser(RelationSchema schema,
                             PliRepository pliRepository,
                             BiConsumer<Vertical, Double> uccConsumer,
                             Vertical prunedColumns,
                             int pruningGraphPartitionCapacity,
                             double maxError,
                             long numTuplePairs) {
        super(schema, pliRepository, uccConsumer, pruningGraphPartitionCapacity, prunedColumns);
        this.maxError = maxError;
        this.numTuplePairs = numTuplePairs;
    }

    @Override
    protected double calculateError(Vertical vertical) {
        PositionListIndex pli = this.pliRepository.getOrCalculateAndCache(vertical);
        if (this.maxError == 0) return pli.size() == 0 ? 0.0 : Double.POSITIVE_INFINITY;
        return pli.getNep() / this.numTuplePairs;
    }

    @Override
    protected double getErrorThreshold() {
        return this.maxError;
    }

}
