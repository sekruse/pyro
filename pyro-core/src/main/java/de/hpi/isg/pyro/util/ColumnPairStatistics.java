package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;

/**
 * This class stores statistics of {@link de.hpi.isg.pyro.model.Column} pairs.
 */
public class ColumnPairStatistics {

    private final Vertical columnPair;
    private final Column column1, column2;
    private PositionListIndex.Volatility volatility;

    public ColumnPairStatistics(Vertical columnPair) {
        if (columnPair.getColumnIndices().cardinality() != 2) {
            throw new IllegalArgumentException();
        }
        this.columnPair = columnPair;
        Column[] columns = columnPair.getColumns();
        this.column1 = columns[0];
        this.column2 = columns[1];
    }

    public Column getColumn1() {
        return column1;
    }

    public Column getColumn2() {
        return column2;
    }

    public Vertical getColumnPair() {
        return this.columnPair;
    }

    public double getCommonNep() {
        return this.columnPair.getNep();
    }

    public double getLowerNep() {
        return Math.min(this.column1.getNep(), this.column2.getNep());
    }

    public double getGreaterNep() {
        return Math.max(this.column1.getNep(), this.column2.getNep());
    }

    public double getExpectedCommonNep() {
        return HyperGeometricDistributions.mean(
                (long) this.column1.getNep(),
                (long) this.column2.getNep(),
                (long) this.getRelation().getMaximumNip()
        );
    }

    /**
     * Get the (estimated) logarithm of the probability that the two {@link Column}s overlap in their equality pairs
     * at least as much as they actually do.
     *
     * @return the estimated natural logarithm of the probability
     */
    public double getLowCommonNepLogPValue() {
        return PFDRater.scaledLogHyperGeometricConfidenceLT.rate(this.column1, this.column2, this.columnPair, this.getRelation());
    }

    /**
     * Get the (estimated) logarithm of the probability that the two {@link Column}s overlap in their equality pairs
     * at least as much as they actually do.
     *
     * @return the estimated natural logarithm of the probability
     */
    public double getHighCommonNepLogPValue() {
        return PFDRater.scaledLogHyperGeometricConfidenceGT.rate(this.column1, this.column2, this.columnPair, this.getRelation());

    }

    public double getCommonNip() {
        return this.column1.getNip() + this.column2.getNip() - this.columnPair.getNip();
    }

    /**
     * Retrieve the {@link de.hpi.isg.pyro.util.PositionListIndex.Volatility} of the {@link Column} pair.
     *
     * @return the {@link de.hpi.isg.pyro.util.PositionListIndex.Volatility}
     */
    public PositionListIndex.Volatility getVolatility() {
        if (this.volatility == null) {
            this.volatility = this.column1.getPositionListIndex().calculateVolatility(this.column2.getPositionListIndex(), false);
        }
        return this.volatility;
    }

    /**
     * Calculate the <i>additional overlap share</i>.
     * <p>First, we figure out how much more overlap in equality pairs (EPs) we find between the two {@link Column}s
     * than we expect. Then, we normalize this to the {@link Column} with less EPs. In other words, this the
     * fraction of unexpected overlapping EPs of the more distinct column.</p>
     *
     * @return the additional overlap share
     * @deprecated Use {@link #getNormalizedEPCorrelation()} instead.
     */
    public double getAdditionalOverlapShare() {
        return (this.getCommonNep() - this.getExpectedCommonNep()) / this.getLowerNep();
    }

    public double getNormalizedEPCorrelation() {
        return Correlations.normalizedCorrelation(this.column1, this.column2, this.columnPair, this.getRelation());
    }

    private Relation getRelation() {
        return columnPair.getRelation();
    }


}
