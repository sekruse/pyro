package de.hpi.isg.pyro.ducc_dfd;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.PositionListIndex;

import java.util.function.BiConsumer;

/**
 * FD graph traversal implementation.
 *
 * @author Sebastian Kruse
 */
public class FdGraphTraverser extends GraphTraverser {

    /**
     * The maximum error for AUCCs.
     */
    protected final double maxError;

    /**
     * The number of tuple pairs in the relation.
     */
    protected final long numTuplePairs;

    /**
     * The right-hand side of FDs to be found.
     */
    private final Column rhs;

    /**
     * Creates a new instance.
     *
     * @param rhs                           the right-hand side of FDs to be found
     * @param schema                        which should be traversed
     * @param pliRepository                 provides {@link PositionListIndex}es
     * @param prunedColumns                 that should not be traversed
     * @param pruningGraphPartitionCapacity see {@link PruningGraph#partitionCapacity}
     * @param maxError                      maximum error for candidates to classify as (partial) UCCs
     * @param numTuplePairs                 the number of tuple pairs in the relation
     */
    public FdGraphTraverser(Column rhs,
                            RelationSchema schema,
                            PliRepository pliRepository,
                            BiConsumer<Vertical, Double> fdConsumer,
                            Vertical prunedColumns,
                            int pruningGraphPartitionCapacity,
                            double maxError,
                            long numTuplePairs) {
        super(schema, pliRepository, fdConsumer, pruningGraphPartitionCapacity, prunedColumns);
        this.rhs = rhs;
        this.maxError = maxError;
        this.numTuplePairs = numTuplePairs;
    }

    @Override
    protected double calculateError(Vertical vertical) {
        PositionListIndex lhsPli = this.pliRepository.getOrCalculateAndCache(vertical);
        PositionListIndex lhsRhsPli = this.pliRepository.getOrCalculateAndCache(vertical.union(this.rhs));
        if (this.maxError == 0) return lhsPli.size() == lhsRhsPli.size() ? 0.0 : Double.POSITIVE_INFINITY;
        return (lhsPli.getNep() - lhsRhsPli.getNep()) / this.numTuplePairs;
    }

    @Override
    protected double getErrorThreshold() {
        return this.maxError;
    }

}
