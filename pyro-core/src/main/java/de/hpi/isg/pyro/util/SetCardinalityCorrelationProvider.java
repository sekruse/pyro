package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;

/**
 * This class uses set cardinality formulas to provide correlation estimates.
 */
public class SetCardinalityCorrelationProvider {

    private final Relation relation;

    private final VerticalMap<Double> equalityPairCache;

    /**
     * Create a new instance.
     *
     * @param relation for that the correlation estimates should be provided
     * @param pliCache cache for {@link PositionListIndex}es
     * @return the new instance
     */
    public static SetCardinalityCorrelationProvider createFor(Relation relation, PLICache pliCache) {
        long startTime = System.currentTimeMillis();

        // Iterate over all column pairs and calculate their common equality pairs.
        VerticalMap<Double> equalityPairCache = new VerticalMap<>(relation);
        for (Column a : relation.getColumns()) {
            for (Column b : relation.getColumns()) {
                if (a.getIndex() >= b.getIndex()) continue;

                // Calculate the intersected PLI.
                Vertical ab = a.union(b);
                PositionListIndex abPLI = ab.getPositionListIndex(pliCache);

                // Cache.
                equalityPairCache.put(ab, abPLI.getNep());
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.printf("done in %,d ms.\n", endTime - startTime);

        return new SetCardinalityCorrelationProvider(relation, equalityPairCache);
    }

    private SetCardinalityCorrelationProvider(Relation relation, VerticalMap<Double> equalityPairCache) {
        this.relation = relation;
        this.equalityPairCache = equalityPairCache;
    }

    /**
     * Estimates the number of tuple pairs that agree in {@code agreement}.
     *
     * @param agreement the {@link Vertical} with agreeing values in the tuple pairs
     * @return the estimate
     */
    public double estimateAgreeingTuplePairs(Vertical agreement) {
        if (agreement.getArity() == 1) return agreement.getPositionListIndex().getNep();
        if (agreement.getArity() == 2) return this.equalityPairCache.get(agreement);

        // Retrieve the pairwise agreements.
        ArrayList<Map.Entry<Vertical, Double>> subsetEntries = this.equalityPairCache.getSubsetEntries(agreement);

        // Sort them by their NEP.
        subsetEntries.sort(Map.Entry.comparingByValue());

        // Take the smallest overlap pair to cover already the first two columns.
        BitSet consideredColumnIndices = new BitSet(this.relation.getNumColumns());
        Map.Entry<Vertical, Double> smallestEntry = subsetEntries.remove(0);
        consideredColumnIndices.or(smallestEntry.getKey().getColumnIndices());
        double p = smallestEntry.getValue() / this.relation.getNumTuplePairs();

        // Iteratively merge to the full result.
        while (consideredColumnIndices.cardinality() < agreement.getArity()) {
            // Find the next column to include.
            // Therefore, we find the first column pair where one column is already considered and the other is not.
            Column nextColumn = null;
            for (Map.Entry<Vertical, Double> subsetEntry : subsetEntries) {
                Vertical columnPair = subsetEntry.getKey();
                if (BitSets.intersectionSize(columnPair.getColumnIndices(), consideredColumnIndices) == 1) {
                    Column[] columns = columnPair.getColumns();
                    if (consideredColumnIndices.get(columns[0].getIndex())) {
                        nextColumn = columns[1];
                    } else {
                        nextColumn = columns[0];
                    }
                    break;
                }
            }
            assert nextColumn != null;

            // Now, basically, we want to estimate the probability:
            // P(tuple pair agrees in considered columns, tuple pair agrees on nextColumn) =
            //   P(tuple pair agrees in considered columns)
            //   * P(tuple pair agrees on nextColumn | tuple pair agrees in considered columns).
            double pConditional = 1d;
            for (Map.Entry<Vertical, Double> subsetEntry : subsetEntries) {
                Vertical columnPair = subsetEntry.getKey();
                if (columnPair.getColumnIndices().get(nextColumn.getIndex()) &&
                        BitSets.intersectionSize(columnPair.getColumnIndices(), consideredColumnIndices) == 1) {
                    pConditional *= subsetEntry.getValue() / this.relation.getNumTuplePairs();
                }
            }
            p *= pConditional;

            // Mark the column as considered.
            consideredColumnIndices.set(nextColumn.getIndex());
        }

        return p * this.relation.getNumTuplePairs();
    }


}
