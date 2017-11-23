package de.hpi.isg.pyro.core;

import com.beust.jcommander.Parameter;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.properties.MetanomeProperty;

import java.io.Serializable;

/**
 * Metanome configuration class for partial FD/UCC algorithms.
 */
public class AbstractPFDConfiguration implements Serializable {

    /**
     * If operated with a {@link de.hpi.isg.mdms.model.MetadataStore}, then we also need the fully qualified name of the
     * {@link de.hpi.isg.mdms.model.targets.Table} on that we operate.
     */
    @Parameter(names = "--tableIdentifier", description = "if operated with Metacrate, the fully qualified name of the " +
            "table being profiled is required")
    @MetanomeProperty
    public String tableIdentifier;

    @Parameter(names = "--constraintCollectionPrefix", description = "if operated with Metacrate, a prefix for a " +
            "constraint collection ID")
    @MetanomeProperty
    public String constraintCollectionPrefix;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // General pruning rules.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * The maximum arity (both for keys and FDs) to consider.
     */
    @Parameter(names = "--maxArity", description = "maximum arity of dependencies to consider (or <1 for no limitation)")
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
    @Parameter(names = "--isFindFds", description = "whether to find FDs", arity = 1)
    @MetanomeProperty
    public boolean isFindFds = true;

    /**
     * The maximum FD error w.r.t. the {@link #fdErrorMeasure}.
     */
    @Parameter(names = "--maxFdError", description = "the maximum allowed FD error")
    @MetanomeProperty
    public double maxFdError = 0.01;


    /**
     * Controls which FD error measure to use.
     */
    @Parameter(names = "--fdErrorMeasure", description = "the FD error measure")
    @MetanomeProperty
    public String fdErrorMeasure = "g1prime";

    /**
     * The lower limit (exclusive) on the EP correlation of two columns to consider them for key-value pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, RelationSchema)
     */
    @MetanomeProperty
    public double minNormalizedEPCorrelationForKeyValuePruning = Double.NaN; //-0.1;

    /**
     * The upper limit (exclusive) on the EP correlation of two columns to consider them for key-value pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, RelationSchema)
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
    @Parameter(names = "--isFindKeys", description = "whether to discover keys/UCCs", arity = 1)
    @MetanomeProperty
    public boolean isFindKeys = true;

    /**
     * Name of the UCC error measure.
     */
    @Parameter(names = "--uccErrorMeasure", description = "the UCC error measure")
    @MetanomeProperty
    public String uccErrorMeasure = "g1prime";

    /**
     * The maximum key error w.r.t. the {@link #uccErrorMeasure}.
     */
    @Parameter(names = "--maxUccError", description = "the maximum allowed UCC error")
    @MetanomeProperty
    public double maxUccError = 0.01;

    /**
     * The lower limit (exclusive) on the EP correlation of two columns to
     * consider them for key-key pruning.
     *
     * @see Correlations#normalizedCorrelation(Vertical, Vertical, Vertical, RelationSchema)
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

    @Parameter(names = "--isNullEqualNull", description = "whether two distinct NULL values should be considered equal")
    @MetanomeProperty
    public boolean isNullEqualNull = true;

    @Parameter(names = "--maxCols", description = "maximum number of columns to profile")
    @MetanomeProperty
    public int maxCols = -1;

    @Parameter(names = "--maxRows", description = "maximum number of rows to profile")
    @MetanomeProperty
    public int maxRows = -1;

}
