package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.mdms.clients.MetacrateClient;
import de.hpi.isg.mdms.domain.constraints.PartialFunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.PartialUniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.experiment.Algorithm;
import de.hpi.isg.mdms.model.experiment.Experiment;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.pyro.model.*;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.properties.MetanomePropertyLedger;
import de.hpi.isg.pyro.util.*;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This class mainly gathers all the configuration functionality for the algorithms that I am developing.
 */
public abstract class AbstractPFDAlgorithm implements FunctionalDependencyAlgorithm, UniqueColumnCombinationsAlgorithm,
        IntegerParameterAlgorithm, StringParameterAlgorithm, RelationalInputParameterAlgorithm, BooleanParameterAlgorithm,
        MetacrateClient {


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Input and output configuration.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private RelationalInputGenerator inputGenerator;
    private static final String INPUT_FILE_CONFIG_KEY = "inputFile";

    FunctionalDependencyResultReceiver fdResultReceiver;

    UniqueColumnCombinationResultReceiver uccResultReceiver;

    /**
     * Can be optionally used to save partial results.
     */
    private MetadataStore metadataStore;

    private Experiment experiment;

    private ConstraintCollection<PartialFunctionalDependency> pfdConstraintcollection;

    private ConstraintCollection<PartialUniqueColumnCombination> puccConstraintCollection;

    /**
     * If operated with a {@link #metadataStore}, then we also need the fully qualified name of the
     * {@link de.hpi.isg.mdms.model.targets.Table} on that we operate.
     */
    @MetanomeProperty
    private String tableIdentifier;

    /**
     * The resolved {@link #tableIdentifier}.
     */
    private Table table;


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
    private double maxCorrelationPValueExponent = 1;

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
     * The maximum FD error w.r.t. the {@link #fdErrorRater}.
     */
    @MetanomeProperty
    double maxFdError = 0.01;


    /**
     * Controls which FD error measure to use.
     */
    @MetanomeProperty
    protected String fdErrorMeasure = "g1prime";

    /**
     * Rates the error of FDs.
     */
    PFDRater fdErrorRater = PFDRater.conflictIpRatioFDErrorRater;

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
     * The maximum key error w.r.t. the {@link #keyErrorRater}.
     */
    @MetanomeProperty
    double maxKeyError = 0.01;

    /**
     * Rates the error of keys.
     */
    PFDRater keyErrorRater = PFDRater.nepPerTupleKeyErrorRater;

    /**
     * Rates the error of keys.
     */
    PFDRater keyScoreRater = (x, a, xa, r) -> r.getNumColumns() - x.getArity();

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

    MetanomePropertyLedger propertyLedger;

    @MetanomeProperty
    boolean isVerbose = false;

    @MetanomeProperty
    boolean isNullEqualNull = true;

    @MetanomeProperty
    private int maxCols = -1;

    @MetanomeProperty
    private int maxRows = -1;



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Working data.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    Relation relation;

    Set<PartialFD> partialFDs;
    PriorityQueue<PartialFD> worstPartialFDs;

    Set<PartialKey> partialKeys;
    PriorityQueue<PartialKey> worstPartialKeys;

    /**
     * Captures the error above which FDs should be pruned due to top-k pruning. If this value is {@link Double#NaN},
     * then it should be ignored, though.
     */
    double fdTopKPruningScore = Double.NaN;

    /**
     * Captures the error above which FDs should be pruned due to top-k pruning. If this value is {@link Double#NaN},
     * then it should be ignored, though.
     */
    double keyTopKPruningScore = Double.NaN;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Methods.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Load the dataset from the {@link #inputGenerator} into the field {@link #relation}.
     */
    void loadDatasetAsColumns() throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        System.out.print("Loading data... ");
        long startTime = System.currentTimeMillis();
        this.relation = ColumnLayoutRelation.createFrom(this.inputGenerator, this.isNullEqualNull, this.maxCols, this.maxRows);
        long endTime = System.currentTimeMillis();
        System.out.printf("%,d ms\n", endTime - startTime);

        this.debug(
                "%s has %d columns, %d rows, and a maximum NIP of %.2f.\n",
                relation.getName(),
                relation.getColumns().size(),
                relation.getNumRows(),
                relation.getMaximumNip()
        );

        // Print a brief summary of the involved columns.
        for (Column column : relation.getColumns()) {
            final double numUniformDistinctValues = InequalityPairs.nipToDistinctValues(column.getNip(), relation.getNumRows());
            final double keyness = numUniformDistinctValues / relation.getNumRows();
            this.debug(
                    "* %s has a keyness of %.2f (~ %.0f distinct values, each appearing %.1fx) and a domination of %.3f.\n",
                    column.getName(),
                    keyness,
                    numUniformDistinctValues, relation.getNumRows() / numUniformDistinctValues,
                    column.getPositionListIndex().getDomination()
            );
        }
        System.out.println();
    }

    /**
     * Load the dataset from the {@link #inputGenerator} into the field {@link #relation}.
     */
    void loadDatasetAsTuples() throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        System.out.print("Loading data... ");
        long startTime = System.currentTimeMillis();
        this.relation = RowLayoutRelation.createFrom(this.inputGenerator, this.isNullEqualNull, this.maxCols, this.maxRows);
        long endTime = System.currentTimeMillis();
        System.out.printf("%,d ms\n", endTime - startTime);

        this.debug(
                "%s has %d columns, %d rows, and a maximum NIP of %.2f.\n",
                relation.getName(),
                relation.getColumns().size(),
                relation.getNumRows(),
                relation.getMaximumNip()
        );

        // Print a brief summary of the involved columns.
        for (Column column : relation.getColumns()) {
            final double numUniformDistinctValues = InequalityPairs.nipToDistinctValues(column.getNip(), relation.getNumRows());
            final double keyness = numUniformDistinctValues / relation.getNumRows();
            this.debug(
                    "* %s has a keyness of %.2f (~ %.0f distinct values, each appearing %.1fx) and a domination of %.3f.\n",
                    column.getName(),
                    keyness,
                    numUniformDistinctValues, relation.getNumRows() / numUniformDistinctValues,
                    column.getPositionListIndex().getDomination()
            );
        }
        System.out.println();
    }

    /**
     * Initialize relevant data structures.
     */
    void initialize() {
        if (this.metadataStore != null) {
            if (this.tableIdentifier == null)
                throw new IllegalStateException("Cannot operate with Metacrate without a table.");
            this.table = this.metadataStore.getTableByName(this.tableIdentifier);
            if (this.table == null)
                throw new IllegalStateException("Invalid table identifier given.");
        }

        this.partialFDs = new HashSet<>();
        this.worstPartialFDs = new PriorityQueue<>(Comparator.comparing(pfd -> -pfd.score));
        this.partialKeys = new HashSet<>();
        this.worstPartialKeys = new PriorityQueue<>(Comparator.comparing(pkey -> -pkey.score));
        this.maxCorrelationLogPValue = HyperGeometricDistributions.toLog(1, this.maxCorrelationPValueExponent);
        switch (this.fdErrorMeasure) {
            case "pdep":
                this.fdErrorRater = PFDRater.conflictIpRatioFDErrorRater;
                break;
            case "g1":
                this.fdErrorRater = PFDRater.g1;
                break;
            case "g1prime":
                this.fdErrorRater = PFDRater.g1prime;
                break;
            default:
                throw new IllegalArgumentException("Unknown FD error measure.");
        }
    }

    /**
     * Probe a {@link PositionListIndex} against a {@link Column}.
     *
     * @param keyPli       the {@link PositionListIndex}
     * @param targetColumn the {@link Column}
     * @return a {@link ProbingResult}
     */
    protected ProbingResult probe(PositionListIndex keyPli, Column targetColumn) {
        if (this.relation instanceof ColumnLayoutRelation) {
            return this.probeWithColumnarLayout(keyPli, targetColumn);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Probe a {@link PositionListIndex} against a {@link Column} assuming that the investigated {@link #relation} is
     * a {@link ColumnLayoutRelation}.
     *
     * @param keyPli       the {@link PositionListIndex}
     * @param targetColumn the {@link Column}
     * @return a {@link ProbingResult}
     */
    protected ProbingResult probeWithColumnarLayout(PositionListIndex keyPli, Column targetColumn) {
        // Probe cluster by cluster.
        double commonNep = 0d;
        for (IntArrayList primaryCluster : keyPli.getIndex()) {
            // Prepare an index to save probing results for the secondary column values.
            Int2IntOpenHashMap refinedClusterSizes = new Int2IntOpenHashMap();
            refinedClusterSizes.defaultReturnValue(0);

            // Do the actual probing of the cluster.
            for (IntIterator clusterIterator = primaryCluster.iterator(); clusterIterator.hasNext(); ) {
                // Fetch the target column value.
                int tupleIndex = clusterIterator.nextInt();
                int targetValue = targetColumn.getData()[tupleIndex];

                // Count the target value occurrence, if it can be part of a non-singleton cluster.
                if (targetValue == Relation.singletonValueId) continue;
                refinedClusterSizes.addTo(targetValue, 1);
            }

            // Now, update the aggregates from the refined cluster sizes.
            ObjectIterator<Int2IntMap.Entry> refinedClusterSizeIterator = refinedClusterSizes.int2IntEntrySet().fastIterator();
            while (refinedClusterSizeIterator.hasNext()) {
                int refinedClusterSize = refinedClusterSizeIterator.next().getIntValue();
                commonNep += refinedClusterSize * (refinedClusterSize - 1d) / 2d;
            }
        }

        return new ProbingResult(commonNep);
    }

    /**
     * Collects data that has been ascertained during a probing.
     */
    public static class ProbingResult {

        final double commonNep;

        public ProbingResult(double commonNep) {
            this.commonNep = commonNep;
        }

    }

    /**
     * Register a {@link PartialKey} to the {@link #partialKeys} and print a brief summary.
     *
     * @return the {@link PartialKey} or {@code null} if none has been added
     */
    synchronized PartialKey registerKey(Vertical keyVertical, double error, double score) {
        // Do the registration.
        PartialKey key = new PartialKey(keyVertical, error, score);
        // this.info("UCC:  %s with error %.32f\n", keyVertical, error);

        if (!this.partialKeys.add(key)) return null;

        // Check whether we have reached our top-k goal of keys.
        if (this.topKKeys > 0 && partialKeys.size() >= this.topKKeys) {
            PartialKey worstKey = this.worstPartialKeys.peek();
            if (worstKey.score < key.score) {
                this.partialKeys.remove(worstKey);
                this.worstPartialKeys.poll();
            } else {
                this.partialKeys.remove(key);
                this.debug("Rejected %s in favor of %s.\n", key, worstKey);
                return null;
            }
        }
        this.worstPartialKeys.add(key);

        // Print the summary (but only if we have the PLI).
        @SuppressWarnings("unused") PositionListIndex keyPli = keyVertical.tryGetPositionListIndex();
        if (this.isVerbose && keyPli != null) {
            Relation relation = keyVertical.getRelation();
            final double numUniformDistinctValues =
                    1d * relation.getNumRows() * relation.getNumRows() /
                            (2 * keyVertical.getNep() + relation.getNumRows());
            final double averageOccurrences = relation.getNumRows() / numUniformDistinctValues;
            this.debug(
                    "Key:  %s (%.0f violating pairs, on average every value occurs %.3fx)\n",
                    key, relation.getMaximumNip() - keyVertical.getNip(), averageOccurrences
            );
        }

        // Feed the result receiver or Metacrate.
        if (this.metadataStore != null) {
            if (this.puccConstraintCollection == null) {
                this.puccConstraintCollection = this.metadataStore.createConstraintCollection(
                        String.format("Partial UCCs from %s (%s: %e, %s)",
                                this.getClass().getSimpleName(),
                                "nepPerTuple",
                                this.maxKeyError,
                                new Date()),
                        this.getOrCreateExperiment(),
                        PartialUniqueColumnCombination.class,
                        this.table
                );
            }
            PartialUniqueColumnCombination partialFunctionalDependency = key.toPartialUniqueColumnCombination(
                    this.metadataStore.getIdUtils(), this.table
            );
            this.puccConstraintCollection.add(partialFunctionalDependency);

        } else {
            try {
                this.uccResultReceiver.receiveResult(key.toMetanomeUniqueColumnCobination());
            } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                // Ignore but notify.
                e.printStackTrace();
            }
        }

        // Update the top-k pruning error.
        if (this.topKKeys > 0 && partialKeys.size() >= this.topKKeys) {
            this.keyTopKPruningScore = this.worstPartialKeys.peek().score;
        }

        return key;
    }


    /**
     * Register a {@link PartialFD} to the {@link #partialFDs} and print a brief summary.
     *
     * @return the added {@link PartialFD} or {@code null} if none was added
     */
    synchronized PartialFD registerFd(Vertical lhs, Column rhs, Vertical jointVertical, double error, double score, boolean isRestricted) {
        // Do the registration.
        PartialFD fd = new PartialFD(lhs, rhs, error, score);
        // this.info("FD:  %s~>%s with error %.32f\n", lhs, rhs, error);

        if (!this.partialFDs.add(fd)) return null;

        // Check whether we have reached our top-k goal of FDs.
        if (this.topKFds > 0 && partialFDs.size() >= this.topKFds) {
            PartialFD worstFd = this.worstPartialFDs.peek();
            if (worstFd.score < fd.score) {
                this.worstPartialFDs.poll();
                this.partialFDs.remove(worstFd);
            } else {
                this.debug("Rejected %s in favor of %s.\n", fd, worstFd);
                this.partialFDs.remove(fd);
                return null;
            }
        }
        this.worstPartialFDs.add(fd);

        // Print the summary (but only if we have the PLIs).
        @SuppressWarnings("unused") PositionListIndex lhsPli = lhs.tryGetPositionListIndex();
        @SuppressWarnings("unused") PositionListIndex jointPli = jointVertical.tryGetPositionListIndex();
        if (this.isVerbose && jointPli != null && lhsPli != null) {
            double numPairs = isRestricted ?
                    lhs.getPositionListIndex().getNepWithout(lhs.getPositionListIndex().getNullCluster()) :
                    lhs.getNep();
            double numViolatedPairs = isRestricted ?
                    jointVertical.getPositionListIndex().getNepWithout(lhs.getPositionListIndex().getNullCluster())
                            - lhs.getPositionListIndex().getNipWithout(lhs.getPositionListIndex().getNullCluster()) :
                    jointVertical.getNip() - lhs.getNip();
            this.debug(
                    "%sFD:  %s (%,.0f of %,.0f pairs violated)\n",
                    isRestricted ? "Restricted " : "", fd, numViolatedPairs, numPairs
            );
            Relation relation = jointVertical.getRelation();
//        this.info("* DV gain error:         %3.3f\n", PFDRater.dvRateFDErrorRater.rate(lhs, rhs, jointVertical, relation));
//        this.info("* NIP error:             %3.3f\n", PFDRater.nipErrorsFDRater.rate(lhs, rhs, jointVertical, relation));
//        this.info("* partial keyness error: %3.3f\n", PFDRater.nipPartialKeynessFDRater.rate(lhs, rhs, jointVertical, relation));
//        this.info("* partial keyne22 error: %3.3f\n", PFDRater.nepPerTuplePartialKeyFDErrorRater.rate(lhs, rhs, jointVertical, relation));
//        this.info("* NIP per tuple error:   %3.3f\n", PFDRater.nipPerTupleFDErrorRater.rate(lhs, rhs, jointVertical, relation));
            this.debug("* IP conflict error: %3.3f\n", PFDRater.conflictIpRatioFDErrorRater.rate(lhs, rhs, jointVertical, relation));
            this.debug("* EP conflict error: %3.3f\n", PFDRater.conflictEpRatioFDErrorRater.rate(lhs, rhs, jointVertical, relation));
        }

        // Feed the result receiver or metadata store.
        if (this.metadataStore != null) {
            if (this.pfdConstraintcollection == null) {
                this.pfdConstraintcollection = this.metadataStore.createConstraintCollection(
                        String.format("Partial FDs from %s (%s: %e, %s)",
                                this.getClass().getSimpleName(),
                                this.fdErrorMeasure,
                                this.maxFdError,
                                new Date()),
                        this.getOrCreateExperiment(),
                        PartialFunctionalDependency.class,
                        this.table
                );
            }
            PartialFunctionalDependency partialFunctionalDependency = fd.toPartialFunctionalDependency(
                    this.metadataStore.getIdUtils(), this.table
            );
            this.pfdConstraintcollection.add(partialFunctionalDependency);
        } else {
            try {
                this.fdResultReceiver.receiveResult(fd.toMetanomeFunctionalDependency());
            } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                // Ignore but notify.
                e.printStackTrace();
            }
        }

        // Update the top-k pruning error.
        if (this.topKFds > 0 && partialFDs.size() >= this.topKFds) {
            this.fdTopKPruningScore = this.worstPartialFDs.peek().score;
        }

        return fd;
    }

    /**
     * Print statistics about the algorithm execution.
     */
    void printSummary() {
        if (!this.isVerbose) return;

        // Print a summary of the discovered keys and FDs.
        Comparator<PartialKey> keyComparator = Comparator.comparingDouble(key -> -key.score);
        keyComparator = keyComparator.thenComparing(key -> key.vertical.getArity());
        List<PartialKey> sortedKeys = new ArrayList<>(this.partialKeys);
        sortedKeys.sort(keyComparator);
        System.out.printf("Discovered %d keys.\n", sortedKeys.size());
        for (PartialKey partialKey : sortedKeys) {
            System.out.printf("* %s\n", partialKey);
        }
        System.out.println();

        Comparator<PartialFD> fdComparator = Comparator.comparingDouble(fd -> -fd.score);
        fdComparator = fdComparator.thenComparing(fd -> fd.lhs.getArity());
        List<PartialFD> sortedFds = new ArrayList<>(this.partialFDs);
        sortedFds.sort(fdComparator);
        System.out.printf("Discovered %d FDs.\n", sortedFds.size());
        for (PartialFD partialFD : sortedFds) {
            System.out.printf("* %s\n", partialFD);
        }
    }

    /**
     * Print statistics about the algorithm execution.
     */
    void printSummaryByRhs() {
        // Print a summary of the discovered keys and FDs.
        System.out.printf("Discovered %d keys.\n", this.partialKeys.size());
        for (PartialKey partialKey : this.partialKeys) {
            System.out.printf("* %s\n", partialKey);
        }
        System.out.println();

        System.out.printf("Discovered %d FDs.\n", this.partialFDs.size());
        int maxArity = this.partialFDs.stream().mapToInt(PartialFD::getArity).max().orElse(0);
        int[] fdsByArity = new int[maxArity + 1];
        Map<Column, List<PartialFD>> partialFDs = new HashMap<>();
        for (PartialFD partialFD : this.partialFDs) {
            partialFDs.computeIfAbsent(partialFD.rhs, column -> new ArrayList<>()).add(partialFD);
        }
        List<List<PartialFD>> partialFDLists = partialFDs.values().stream()
                .peek(pfds -> pfds.sort(PartialFD.scoreComparator.reversed()))
                .sorted((pfds1, pfds2) -> PartialFD.scoreComparator.reversed().compare(pfds1.get(0), pfds2.get(0)))
                .collect(Collectors.toList());
        for (List<PartialFD> partialFDList : partialFDLists) {
            Column rhs = partialFDList.get(0).rhs;
            System.out.printf("* Partial FDs for RHS %s:\n", rhs);
            partialFDList.sort(PartialFD.scoreComparator.reversed());
            for (PartialFD partialFD : partialFDList) {
                PositionListIndex.Volatility volatility =
                        partialFD.lhs.getPositionListIndex().calculateVolatility(partialFD.rhs.getPositionListIndex(), false);
                System.out.printf("  * %s (%s)\n", partialFD, volatility);
                fdsByArity[partialFD.lhs.getArity()]++;
            }
            System.out.println();
        }
        for (int arity = 1; arity < fdsByArity.length; arity++) {
            System.out.printf("* %5d FDs of arity %d.\n", fdsByArity[arity], arity);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Key-key pruning.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Tells whether the configuration allows for key-key pruning.
     */
    boolean shouldApplyKeyKeyPruning() {
        return -1d <= this.minNormalizedEPCorrelationForKeyKeyPruning && this.minNormalizedEPCorrelationForKeyKeyPruning < 1;
    }

    /**
     * Check whether two {@link Column}s may be combined to form interesting key candidates.
     *
     * @param columnPairStatistics of the two {@link Column}s
     * @return whether they are combinable
     */
    protected boolean isKeyKeyPruned(ColumnPairStatistics columnPairStatistics) {
        // Now, the EPs should not overlap much more than they would at random.
        double additionalKeyOverlapShare = columnPairStatistics.getNormalizedEPCorrelation();
        return additionalKeyOverlapShare > this.minNormalizedEPCorrelationForKeyKeyPruning;
    }

    /**
     * Check whether two {@link Column}s may be combined to form interesting key candidates.
     *
     * @param a  the first {@link Column}
     * @param b  the second {@link Column}
     * @param ab the combination of {@code a} and {@code b}
     * @return whether they are combinable
     */
    protected boolean isKeyKeyPruned(Column a, Column b, Vertical ab) {
        // Now, the EPs should not overlap much more than they would at random.
        double normalizedCorrelation = Correlations.normalizedCorrelation(a, b, ab, this.relation);
        return normalizedCorrelation > this.minNormalizedEPCorrelationForKeyKeyPruning;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Key-value pruning.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * Tells whether the configuration allows for key-value pruning.
     */
    boolean shouldApplyKeyValuePruning() {
        boolean isValidLowerBound = this.minNormalizedEPCorrelationForKeyValuePruning < 1;
        boolean isValidUpperBound = this.maxNormalizedEPCorrelationForKeyValuePruning > -1;
        boolean isBoundsCompatible = this.minNormalizedEPCorrelationForKeyValuePruning < this.maxNormalizedEPCorrelationForKeyValuePruning;
        return isValidLowerBound && isValidUpperBound && isBoundsCompatible;
    }


    /**
     * Check whether two {@link Column}s may be combined to form interesting embedded FDs.
     *
     * @param columnPairStatistics of the two {@link Column}s
     * @return whether they are combinable
     */
    protected boolean isKeyValuePruned(ColumnPairStatistics columnPairStatistics) {
        // Now, the EPs should not overlap much more than they would at random.
        double additionalOverlapShare = columnPairStatistics.getNormalizedEPCorrelation();
        boolean isCorrelated = additionalOverlapShare >= this.minNormalizedEPCorrelationForKeyValuePruning;
        boolean isNegativeCorrelated = -additionalOverlapShare >= this.minNormalizedEPCorrelationForKeyValuePruning;

        if (isCorrelated) {
            double logPValue = columnPairStatistics.getHighCommonNepLogPValue();
            return logPValue > this.maxCorrelationLogPValue;
        } else if (isNegativeCorrelated) {
            double logPValue = columnPairStatistics.getLowCommonNepLogPValue();
            return logPValue > this.maxCorrelationLogPValue;
        } else {
            return true;
        }
    }

    /**
     * Check whether two {@link Column}s may be combined to form interesting embedded FDs.
     *
     * @param a  the first {@link Column}
     * @param b  the second {@link Column}
     * @param ab the combination of {@code a} and {@code b}
     * @return whether they are combinable
     */
    protected boolean isKeyValuePruned(Column a, Column b, Vertical ab) {
        // Now, the EPs should not overlap much more than they would at random.
        double normalizedCorrelation = Correlations.normalizedCorrelation(a, b, ab, this.relation);
        boolean isCorrelated = normalizedCorrelation >= this.minNormalizedEPCorrelationForKeyValuePruning;
        boolean isNegativeCorrelated = normalizedCorrelation <= this.minNormalizedEPCorrelationForKeyValuePruning;

        if (isCorrelated) {
            double logPValue = PFDRater.scaledLogHyperGeometricConfidenceGT.rate(a, b, ab, this.relation);
            return logPValue > this.maxCorrelationLogPValue;
        } else if (isNegativeCorrelated) {
            double logPValue = PFDRater.scaledLogHyperGeometricConfidenceLT.rate(a, b, ab, this.relation);
            return logPValue > this.maxCorrelationLogPValue;
        } else {
            return true;
        }
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Key-key-value pruning.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    boolean shouldApplyKeyKeyValuePruning() {
        return -1d <= this.maxNormalizedIPConditionalCorrelationForKeyKeyValuePruning
                && this.maxNormalizedIPConditionalCorrelationForKeyKeyValuePruning < 1;
    }


    /**
     * Check for two embedded FDs {@code A~>C} and {@code B~>C}, whether there contribution to the determination of {@code C} is
     * sufficiently dissimilar to consider the FD candidate {@code AB->C}
     *
     * @return whether they are sufficiently dissimilar
     */
    protected boolean isKeyKeyValuePruned(Vertical lhs,
                                          Column rhs,
                                          ColumnCombinationIndex<ColumnPairStatistics> columnPairStatistics,
                                          LatticeVertex vertex) {
        assert lhs.getArity() == 2;

        Column[] lhsColumns = lhs.getColumns();
        ColumnPairStatistics parentStats1 = columnPairStatistics.get(lhsColumns[0], rhs);
        ColumnPairStatistics parentStats2 = columnPairStatistics.get(lhsColumns[1], rhs);

        double normalizedConditionalCorrelation = Correlations.normalizedIPConditionalCorrelation(
                lhsColumns[0],
                lhsColumns[1],
                rhs,
                lhs,
                parentStats1.getColumnPair(),
                parentStats2.getColumnPair(),
                vertex.getVertical()
        );

        return normalizedConditionalCorrelation > this.maxNormalizedIPConditionalCorrelationForKeyKeyValuePruning;
    }

    /**
     * Check for two embedded FDs {@code A~>C} and {@code B~>C}, whether there contribution to the determination of {@code C} is
     * sufficiently dissimilar to consider the FD candidate {@code AB->C}
     *
     * @return whether they are sufficiently dissimilar
     */
    protected boolean isKeyKeyValuePruned(Column a, Column b, Column c, Vertical ab, Vertical ac, Vertical bc, Vertical abc) {
        double normalizedConditionalCorrelation = Correlations.normalizedIPConditionalCorrelation(a, b, c, ab, ac, bc, abc);
        return normalizedConditionalCorrelation > this.maxNormalizedIPConditionalCorrelationForKeyKeyValuePruning;
    }


    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> configurationRequirement = new ArrayList<>();
        try {
            this.getPropertyLedger().contributeConfigurationRequirements(configurationRequirement);
        } catch (AlgorithmConfigurationException e) {
            throw new RuntimeException(e);
        }
        {
            ConfigurationRequirementFileInput requirement = new ConfigurationRequirementFileInput(INPUT_FILE_CONFIG_KEY);
            requirement.setRequired(true);
            configurationRequirement.add(requirement);
        }
        return configurationRequirement;
    }

    @Override
    public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
        this.fdResultReceiver = resultReceiver;
    }

    @Override
    public void setResultReceiver(UniqueColumnCombinationResultReceiver resultReceiver) {
        this.uccResultReceiver = resultReceiver;
    }

    @Override
    public void setMetadataStore(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

    private Experiment getOrCreateExperiment() {
        if (this.experiment == null) {
            Algorithm algorithm = this.metadataStore.createAlgorithm(this.getClass().getCanonicalName());
            this.experiment = this.metadataStore.createExperiment("Profiling partial FDs/UCCs", algorithm);
            try {
                this.getPropertyLedger().getProperties().forEach(
                        (key, value) -> {
                            try {
                                this.experiment.addParameter(key, String.valueOf(value.get(this)));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        }
                );
            } catch (Exception e) {
                System.err.println("Could not log experiment parameters properly.");
                e.printStackTrace();
            }
        }
        return this.experiment;
    }

    @Override
    public void setStringConfigurationValue(String identifier, String... values)
            throws AlgorithmConfigurationException {

        if (this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format(
                "Unknown string parameter: \"%s\"", identifier
        ));
    }

    @Override
    public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
        if (this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format(
                "Unknown Boolean parameter: \"%s\"", identifier
        ));
    }


    @Override
    public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
        if (this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format(
                "Unknown Boolean parameter: \"%s\"", identifier
        ));
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier,
                                                     RelationalInputGenerator... values)
            throws AlgorithmConfigurationException {
        switch (identifier) {
            case INPUT_FILE_CONFIG_KEY:
                if (values.length != 1) throw new AlgorithmConfigurationException("Only one input file supported.");
                this.inputGenerator = values[0];
                break;

            default:
                throw new AlgorithmConfigurationException(String.format(
                        "Unknown file input parameter: \"%s\"", identifier
                ));
        }
    }

    private MetanomePropertyLedger getPropertyLedger() throws AlgorithmConfigurationException {
        if (this.propertyLedger == null) {
            this.propertyLedger = MetanomePropertyLedger.createFor(this);
        }
        return propertyLedger;
    }

    final void debug(String msg, Object... params) {
        if (this.isVerbose) {
            System.out.printf(msg, params);
        }
    }

    final void info(String msg, Object... params) {
        if (this.isVerbose) {
            System.out.printf(msg, params);
        }
    }

}
