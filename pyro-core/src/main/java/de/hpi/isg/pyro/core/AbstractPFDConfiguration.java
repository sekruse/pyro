package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.util.Correlations;

/**
 * Metanome configuration class for partial FD/UCC algorithms.
 */
public class AbstractPFDConfiguration {

    /**
     * If operated with a {@link de.hpi.isg.mdms.model.MetadataStore}, then we also need the fully qualified name of the
     * {@link de.hpi.isg.mdms.model.targets.Table} on that we operate.
     */
    @MetanomeProperty
    public String tableIdentifier;


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // General pruning rules.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * The maximum arity (both for keys and FDs) to consider.
     */
    @MetanomeProperty
    public int maxArity = -1;

    /**
     * Defines the {@link #maxCorrelationLogPValue} to be {@code 10^x}, where {@code x} is this value.
     */
    @MetanomeProperty
    public double maxCorrelationPValueExponent = 1;

    /**
     * Designates when a correlation of two columns is considered statically relevant w.r.t. a hyper-geometric test.
     */
    public double maxCorrelationLogPValue;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // FD pruning rules.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Whether to find FDs.
     */
    @MetanomeProperty
    public boolean isFindFds = true;

    /**
     * The maximum FD error w.r.t. the {@link #fdErrorMeasure}.
     */
    @MetanomeProperty
    public double maxFdError = 0.01;


    /**
     * Controls which FD error measure to use.
     */
    @MetanomeProperty
    public String fdErrorMeasure = "g1prime";

    /**
     * The lower limit (exclusive) on the EP correlation of two columns to consider them for key-value pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, Relation)
     */
    @MetanomeProperty
    public double minNormalizedEPCorrelationForKeyValuePruning = Double.NaN; //-0.1;

    /**
     * The upper limit (exclusive) on the EP correlation of two columns to consider them for key-value pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, Relation)
     */
    @MetanomeProperty
    public double maxNormalizedEPCorrelationForKeyValuePruning = Double.NaN; //0.1;

    /**
     * The lower limit (exclusive) on the conditional IP correlation of two key columns w.r.t. a value column to
     * consider them for key-key-value pruning.
     *
     * @see Correlations#normalizedIPConditionalCorrelation(Vertical, Vertical, Vertical, Vertical, Vertical, Vertical, Vertical)
     */
    @MetanomeProperty
    public double maxNormalizedIPConditionalCorrelationForKeyKeyValuePruning = Double.NaN; //.9;

    /**
     * Whether supersets of partial keys should be considered.
     */
    @MetanomeProperty
    public boolean isFindOnlyMinimalPFDs = true;

    /**
     * Whether FDs should be found that hold where there key is not {@code null}.
     */
    @MetanomeProperty
    public boolean isFindRestrictedFDs = false;

    /**
     * Whether partial FDs should be used for transitive pruning.
     */
    @MetanomeProperty
    public boolean isPruningWithPFDs = false;

    /**
     * Whether partial keys should not serve as LHS of partial FDs.
     */
    @MetanomeProperty
    public boolean isPruningPFDsWithPKeys = false;

    /**
     * Designates the maximum number of partial FDs to be discovered (according to some ranking criterion).
     */
    @MetanomeProperty
    public int topKFds = -1;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Key pruning rules.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Whether to find keys.
     */
    @MetanomeProperty
    public boolean isFindKeys = true;

    /**
     * Name of the UCC error measure.
     */
    @MetanomeProperty
    public String uccErrorMeasure = "g1prime";

    /**
     * The maximum key error w.r.t. the {@link #uccErrorMeasure}.
     */
    @MetanomeProperty
    public double maxUccError = 0.01;

    /**
     * The lower limit (exclusive) on the EP correlation of two columns to
     * consider them for key-key pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, Relation)
     */
    @MetanomeProperty
    public double minNormalizedEPCorrelationForKeyKeyPruning = Double.NaN; // 0.5;

    /**
     * Whether supersets of partial FDs should be considered.
     */
    @MetanomeProperty
    public boolean isFindOnlyMinimalPKs = true;

    /**
     * Designates the maximum number of partial FDs to be discovered (according to some ranking criterion).
     */
    @MetanomeProperty
    public int topKKeys = -1;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscellaneous configuration.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    public boolean isVerbose = false;

    @MetanomeProperty
    public boolean isNullEqualNull = true;

    @MetanomeProperty
    public int maxCols = -1;

    @MetanomeProperty
    public int maxRows = -1;

}
