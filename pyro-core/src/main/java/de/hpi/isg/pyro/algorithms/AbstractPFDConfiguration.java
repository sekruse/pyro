package de.hpi.isg.pyro.algorithms;

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
    String tableIdentifier;


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // General pruning rules.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * The maximum arity (both for keys and FDs) to consider.
     */
    @MetanomeProperty
    int maxArity = -1;

    /**
     * Defines the {@link #maxCorrelationLogPValue} to be {@code 10^x}, where {@code x} is this value.
     */
    @MetanomeProperty
    double maxCorrelationPValueExponent = 1;

    /**
     * Designates when a correlation of two columns is considered statically relevant w.r.t. a hyper-geometric test.
     */
    double maxCorrelationLogPValue;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // FD pruning rules.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Whether to find FDs.
     */
    @MetanomeProperty
    boolean isFindFds = true;

    /**
     * The maximum FD error w.r.t. the {@link #fdErrorMeasure}.
     */
    @MetanomeProperty
    double maxFdError = 0.01;


    /**
     * Controls which FD error measure to use.
     */
    @MetanomeProperty
    String fdErrorMeasure = "g1prime";

    /**
     * The lower limit (exclusive) on the EP correlation of two columns to consider them for key-value pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, Relation)
     */
    @MetanomeProperty
    double minNormalizedEPCorrelationForKeyValuePruning = Double.NaN; //-0.1;

    /**
     * The upper limit (exclusive) on the EP correlation of two columns to consider them for key-value pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, Relation)
     */
    @MetanomeProperty
    double maxNormalizedEPCorrelationForKeyValuePruning = Double.NaN; //0.1;

    /**
     * The lower limit (exclusive) on the conditional IP correlation of two key columns w.r.t. a value column to
     * consider them for key-key-value pruning.
     *
     * @see Correlations#normalizedIPConditionalCorrelation(Vertical, Vertical, Vertical, Vertical, Vertical, Vertical, Vertical)
     */
    @MetanomeProperty
    double maxNormalizedIPConditionalCorrelationForKeyKeyValuePruning = Double.NaN; //.9;

    /**
     * Whether supersets of partial keys should be considered.
     */
    @MetanomeProperty
    boolean isFindOnlyMinimalPFDs = true;
    /**
     * Whether FDs should be found that hold where there key is not {@code null}.
     */
    @MetanomeProperty
    boolean isFindRestrictedFDs = false;

    /**
     * Whether partial FDs should be used for transitive pruning.
     */
    @MetanomeProperty
    boolean isPruningWithPFDs = false;

    /**
     * Whether partial keys should not serve as LHS of partial FDs.
     */
    @MetanomeProperty
    boolean isPruningPFDsWithPKeys = false;

    /**
     * Designates the maximum number of partial FDs to be discovered (according to some ranking criterion).
     */
    @MetanomeProperty
    int topKFds = -1;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Key pruning rules.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Whether to find keys.
     */
    @MetanomeProperty
    boolean isFindKeys = true;

    /**
     * Name of the UCC error measure.
     */
    @MetanomeProperty
    String uccErrorMeasure = "g1prime";

    /**
     * The maximum key error w.r.t. the {@link #uccErrorMeasure}.
     */
    @MetanomeProperty
    double maxUccError = 0.01;

    /**
     * The lower limit (exclusive) on the EP correlation of two columns to
     * consider them for key-key pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, Relation)
     */
    @MetanomeProperty
    double minNormalizedEPCorrelationForKeyKeyPruning = Double.NaN; // 0.5;

    /**
     * Whether supersets of partial FDs should be considered.
     */
    @MetanomeProperty
    boolean isFindOnlyMinimalPKs = true;

    /**
     * Designates the maximum number of partial FDs to be discovered (according to some ranking criterion).
     */
    @MetanomeProperty
    int topKKeys = -1;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscellaneous configuration.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    boolean isVerbose = false;

    @MetanomeProperty
    boolean isNullEqualNull = true;

    @MetanomeProperty
    int maxCols = -1;

    @MetanomeProperty
    int maxRows = -1;

}
