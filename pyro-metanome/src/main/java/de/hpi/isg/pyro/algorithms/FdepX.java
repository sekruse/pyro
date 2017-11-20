package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.mdms.clients.MetacrateClient;
import de.hpi.isg.mdms.domain.constraints.PartialFunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.PartialUniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.pyro.core.AbstractPFDConfiguration;
import de.hpi.isg.pyro.core.DependencyConsumer;
import de.hpi.isg.pyro.core.SearchSpace;
import de.hpi.isg.pyro.fdep.FdTree;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.properties.MetanomePropertyLedger;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementFileInput;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;

/**
 * This is an implementation of the FDEP algorithm. More specifically, this implementation discovers partial FDs
 * (according to the g1 measure) by
 * <ol>
 * <li>building the negative cover of a relation by comparing all tuple pairs,</li>
 * <li>removing from the negative cover those non-FDs that are violated only few times (+ redundant non-FDs), and</li>
 * <li>inverting the pruned negative cover.</li>
 * </ol>
 */
public class FdepX
        extends DependencyConsumer
        implements FunctionalDependencyAlgorithm, UniqueColumnCombinationsAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, RelationalInputParameterAlgorithm, BooleanParameterAlgorithm,
        MetacrateClient {

    public static final String INPUT_FILE_CONFIG_KEY = "inputFile";

    private RelationalInputGenerator inputGenerator;

    private MetanomePropertyLedger propertyLedger;
    private final FdepX.Configuration configuration = new FdepX.Configuration();

    private MetadataStore metadataStore;
    private Table table;
    private ConstraintCollection<PartialFunctionalDependency> pfdConstraintcollection;
    private ConstraintCollection<PartialUniqueColumnCombination> puccConstraintcollection;
    private ProfilingData profilingData;

    @Override
    public void execute() throws AlgorithmExecutionException {
        if (!this.configuration.isFindFds && !this.configuration.isFindKeys) {
            throw new AlgorithmExecutionException("Told to find neither FDs nor UCCs.");
        }
        if (this.configuration.isFindFds && this.configuration.isFindKeys && this.configuration.maxFdError != this.configuration.maxUccError) {
            throw new AlgorithmExecutionException("Cannot process different FD and UCC errors.");
        }
        double maxError = this.configuration.isFindFds ? this.configuration.maxFdError : this.configuration.maxUccError;
        this.profilingData = new ProfilingData();

        // Load the input file.
        this.profilingData.overallMillis = System.currentTimeMillis();
        this.profilingData.initializationMillis = System.currentTimeMillis();
        ArrayList<List<String>> relation = new ArrayList<>();
        RelationSchema relationSchema;
        try {
            try (RelationalInput relationalInput = this.inputGenerator.generateNewCopy()) {
                relationSchema = new RelationSchema(relationalInput.relationName(), this.configuration.isNullEqualNull);
                for (String columnName : relationalInput.columnNames()) {
                    relationSchema.appendColumn(columnName);
                }
                while (relationalInput.hasNext()) {
                    List<String> row = relationalInput.next();
                    relation.add(row);
                }
                if (relation.isEmpty()) return;
            }
        } catch (AlgorithmExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new AlgorithmExecutionException("Failed to load the relation.", e);
        }
        this.profilingData.initializationMillis = System.currentTimeMillis() - this.profilingData.initializationMillis;

        // ---------------------------------------------------------------------------------------------------------- //
        // Build the negative cover.
        // ---------------------------------------------------------------------------------------------------------- //
        this.profilingData.negativeCoverCalculationMillis = System.currentTimeMillis();
        long numAllTuplePairs = relation.size() * (relation.size() - 1L) / 2;
        long startMillis = System.currentTimeMillis();
        long nextUpdateMillis = startMillis + 1000L;
        int percentOfComparedTuplePairs = 0;
        long numComparedTuplePairs = 0;
        FdTree negativeCover = new FdTree(relationSchema.getNumColumns() + 1); // +1: We add an artificial key attribute.
        for (int i = 0; i < relation.size(); i++) {
            List<String> t1 = relation.get(i);
            for (int j = i + 1; j < relation.size(); j++) {
                List<String> t2 = relation.get(j);

                BitSet agreeSet = new BitSet(t1.size());
                BitSet diffSet = new BitSet(t1.size() + 1);
                for (int k = 0; k < t1.size(); k++) {
                    if (Objects.equals(t1.get(k), t2.get(k))) agreeSet.set(k);
                    else if (this.configuration.isFindFds) diffSet.set(k);
                }
                if (this.configuration.isFindKeys)
                    diffSet.set(relationSchema.getNumColumns()); // The artificial key attribute...
                negativeCover.add(agreeSet, diffSet);

                // Output current status.
                numComparedTuplePairs++;
                long currentMillis = System.currentTimeMillis();
                if (currentMillis >= nextUpdateMillis) {
                    long elapsedMillis = Math.max(currentMillis - startMillis, 1L);
                    double tuplesPerMilli = numComparedTuplePairs / elapsedMillis;
                    long numRemainingTuples = numAllTuplePairs - numComparedTuplePairs;
                    long remainingMillis = (long) (numRemainingTuples / tuplesPerMilli);

                    percentOfComparedTuplePairs = (int) (numComparedTuplePairs * 100 / numAllTuplePairs);
                    System.out.printf(
                            "\rCompared tuple pairs: %3d%% (remaining time: ~%-30s",
                            percentOfComparedTuplePairs,
                            formatDuration(remainingMillis) + ")..."
                    );
                    nextUpdateMillis = currentMillis + 5000L;
                }
            }
        }
        System.out.println();
        this.profilingData.negativeCoverCalculationMillis = System.currentTimeMillis() - this.profilingData.negativeCoverCalculationMillis;
        this.profilingData.negativeCoverSize = negativeCover.countNodes();

        // ---------------------------------------------------------------------------------------------------------- //
        // Trim the negative cover.
        // ---------------------------------------------------------------------------------------------------------- //
        if (maxError > 0.0) {
            this.profilingData.trimCoverMillis = System.currentTimeMillis();
            System.out.printf("Trimming the negative cover (%,d nodes)...\n", this.profilingData.negativeCoverSize);
            long numTuplePairs = relation.size() * (relation.size() - 1L) / 2;
            long maxViolations = (long) (maxError * numTuplePairs);
            negativeCover = negativeCover.prune(maxViolations);
            this.profilingData.trimCoverMillis = System.currentTimeMillis() - this.profilingData.trimCoverMillis;
            this.profilingData.trimmedNegativeCoverSize = negativeCover.countNodes();
        } else {
            this.profilingData.trimmedNegativeCoverSize = this.profilingData.negativeCoverSize;
        }

        // ---------------------------------------------------------------------------------------------------------- //
        // Induce the positive cover.
        // ---------------------------------------------------------------------------------------------------------- //
        this.profilingData.coverInversionMillis = System.currentTimeMillis();
        System.out.println("Inducing the positive cover...");
        // Create the positive cover.
        FdTree positiveCover = new FdTree(relationSchema.getNumColumns() + 1);
        // Handle the RHS individually.
        BitSet rhsBitSet = new BitSet(relationSchema.getNumColumns());
        for (int rhs = 0; rhs < relationSchema.getNumColumns() + 1; rhs++) {
            // Determine whether the RHS is a normal attribute or the artificial key.
            boolean isFdRhs = rhs < relationSchema.getNumColumns();
            if (isFdRhs && !this.configuration.isFindFds || !isFdRhs && !this.configuration.isFindKeys) continue;

            rhsBitSet.set(rhs);
            // Add the most general FD as initial hypothesis.
            positiveCover.add(new BitSet(0), rhsBitSet);
            // Specify it for each non-FD.
            for (BitSet nonFdLhs : negativeCover.getAllLhs(rhs)) {
                // Remove all violated FDs.
                Collection<BitSet> removedLhss = positiveCover.removeGeneralizations(nonFdLhs, rhs);
                for (BitSet removedLhs : removedLhss) {
                    // Specialize all removed FDs and insert them if they are not already covered.
                    for (int additionalAttribute = nonFdLhs.nextClearBit(0);
                         additionalAttribute != -1 && additionalAttribute < relationSchema.getNumColumns();
                         additionalAttribute = nonFdLhs.nextClearBit(additionalAttribute + 1)) {
                        // Make sure to avoid trivial FDs.
                        if (additionalAttribute == rhs) continue;
                        removedLhs.set(additionalAttribute);
                        if (!positiveCover.containsGeneralization(removedLhs, rhs)) {
                            positiveCover.add((BitSet) removedLhs.clone(), rhsBitSet);
                        }
                        removedLhs.clear(additionalAttribute);
                    }
                }
            }
            rhsBitSet.clear(rhs);
            // Output all the FDs for the RHS.
            for (BitSet lhs : positiveCover.getAllLhs(rhs)) {
                if (isFdRhs) {
                    this.registerFd(
                            relationSchema.getVertical(lhs),
                            relationSchema.getColumn(rhs),
                            this.configuration.isCalculateErrors ?
                                    this.calculateFdG1Error(relation, lhs, rhs) :
                                    Double.NaN,
                            Double.NaN
                    );
                } else {
                    // If the RHS is the artifical key attribute, then we can interpret the LHS as keys.
                    this.registerUcc(
                            relationSchema.getVertical(lhs),
                            this.configuration.isCalculateErrors ?
                                    this.calculateUccG1Error(relation, lhs) :
                                    Double.NaN,
                            Double.NaN
                    );
                }
                this.profilingData.numDependencies++;
            }
        }
        this.profilingData.coverInversionMillis = System.currentTimeMillis()
                - this.profilingData.coverInversionMillis
                - this.profilingData.errorCalculationNanos / 1_000_000;
        this.profilingData.overallMillis = System.currentTimeMillis() - this.profilingData.overallMillis;
        this.profilingData.printReport("FdepX", System.out);
    }

    private double calculateFdG1Error(ArrayList<List<String>> relation, BitSet lhs, int rhs) {
        final long startNanos = System.nanoTime();
        Map<List<String>, Object2IntOpenHashMap<String>> grouping = new HashMap<>();
        for (List<String> tuple : relation) {
            grouping.computeIfAbsent(
                    projectTuple(tuple, lhs),
                    key -> new Object2IntOpenHashMap<>()
            ).addTo(tuple.get(rhs), 1);
        }
        long numViolatingTuplePairs = 0L;
        for (Object2IntOpenHashMap<String> group : grouping.values()) {
            if (group.size() == 1) continue;
            long numSatisfyingGroupTuplePairs = group.size() * (group.size() - 1L);
            int numGroupTuples = 0;
            for (IntIterator iterator = group.values().iterator();
                 iterator.hasNext(); ) {
                long subgroupSize = iterator.nextInt();
                numSatisfyingGroupTuplePairs += subgroupSize * (subgroupSize - 1L);
                numGroupTuples += subgroupSize;
            }
            numViolatingTuplePairs += numGroupTuples * (numGroupTuples - 1L) - numSatisfyingGroupTuplePairs;
        }
        double error = numViolatingTuplePairs / (double) (relation.size() * (relation.size() - 1L));
        this.profilingData.errorCalculationNanos = System.nanoTime() - startNanos;
        return error;
    }

    private double calculateUccG1Error(ArrayList<List<String>> relation, BitSet columns) {
        final long startNanos = System.nanoTime();
        Object2IntOpenHashMap<List<String>> counter = new Object2IntOpenHashMap<>();
        for (List<String> tuple : relation) {
            counter.addTo(projectTuple(tuple, columns), 1);
        }
        long numViolatingTuplePairs = 0L;
        for (IntIterator iterator = counter.values().iterator();
             iterator.hasNext(); ) {
            long groupSize = iterator.nextInt();
            numViolatingTuplePairs += groupSize * (groupSize - 1L);
        }

        double error = numViolatingTuplePairs / (double) (relation.size() * (relation.size() - 1L));
        this.profilingData.errorCalculationNanos = System.nanoTime() - startNanos;
        return error;
    }

    private static List<String> projectTuple(List<String> tuple, BitSet columnIndices) {
        List<String> projection = new ArrayList<>(columnIndices.cardinality());
        for (int i = columnIndices.nextSetBit(0);
             i != -1;
             i = columnIndices.nextSetBit(i + 1)) {
            projection.add(tuple.get(i));
        }
        return projection;
    }

    /**
     * Format the given milliseconds as {@code h:mm:ss.mmm}.
     *
     * @param millis milliseconds
     * @return the formatted {@link String}
     */
    private static String formatDuration(long millis) {
        long ms = millis % 1000;
        millis /= 1000;
        long s = millis % 60;
        millis /= 60;
        long min = millis % 60;
        millis /= 60;
        long h = millis;
        return String.format("%d:%02d:%02d.%03d", h, min, s, ms);
    }

    @Override
    public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
        if (this.metadataStore != null) return;

        this.fdConsumer = partialFD -> {
            try {
                resultReceiver.receiveResult(partialFD.toMetanomeFunctionalDependency());
            } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                throw new RuntimeException(String.format("Could not receive %s.", partialFD), e);
            }
        };
    }

    @Override
    public void setResultReceiver(UniqueColumnCombinationResultReceiver resultReceiver) {
        if (this.metadataStore != null) return;

        this.uccConsumer = partialKey -> {
            try {
                resultReceiver.receiveResult(partialKey.toMetanomeUniqueColumnCobination());
            } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                throw new RuntimeException(String.format("Could not receive %s.", partialKey), e);
            }
        };
    }


    @Override
    public void setMetadataStore(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;

        // If we are given a MetadataStore, then we bypass the result receivers, because they do not support
        // approximate/partial dependencies.
        this.fdConsumer = partialFD -> {
            if (this.pfdConstraintcollection == null) {
                this.pfdConstraintcollection = this.metadataStore.createConstraintCollection(
                        String.format("Partial FDs from %s (%s)", this.getClass().getSimpleName(), new Date()),
                        PartialFunctionalDependency.class,
                        this.table
                );
            }
            PartialFunctionalDependency partialFunctionalDependency = partialFD.toPartialFunctionalDependency(
                    this.metadataStore.getIdUtils(), this.table
            );
            this.pfdConstraintcollection.add(partialFunctionalDependency);
        };
        this.uccConsumer = partialKey -> {
            if (this.puccConstraintcollection == null) {
                this.puccConstraintcollection = this.metadataStore.createConstraintCollection(
                        String.format("Partial UCCs from %s (%s)", this.getClass().getSimpleName(), new Date()),
                        PartialUniqueColumnCombination.class,
                        this.table
                );
            }
            PartialUniqueColumnCombination partialUniqueColumnCombination = partialKey.toPartialUniqueColumnCombination(
                    this.metadataStore.getIdUtils(), this.table
            );
            this.puccConstraintcollection.add(partialUniqueColumnCombination);
        };
    }

    public MetanomePropertyLedger getPropertyLedger() {
        if (this.propertyLedger == null) {
            try {
                this.propertyLedger = MetanomePropertyLedger.createFor(this.configuration);
            } catch (AlgorithmConfigurationException e) {
                throw new RuntimeException("Could not initialize property ledger.", e);
            }
        }
        return propertyLedger;
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> configurationRequirement = new ArrayList<>();
        {
            ConfigurationRequirementFileInput requirement = new ConfigurationRequirementFileInput(INPUT_FILE_CONFIG_KEY);
            requirement.setRequired(true);
            configurationRequirement.add(requirement);
        }
        this.getPropertyLedger().contributeConfigurationRequirements(configurationRequirement);

        return configurationRequirement;
    }

    public void setStringConfigurationValue(String identifier, String... values)
            throws AlgorithmConfigurationException {

        if (this.getPropertyLedger().configure(this.configuration, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format("Unknown string parameter: \"%s\"", identifier));
    }

    @Override
    public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
        if (this.getPropertyLedger().configure(this.configuration, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format("Unknown Boolean parameter: \"%s\"", identifier));
    }


    @Override
    public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
        if (this.getPropertyLedger().configure(this.configuration, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format("Unknown Boolean parameter: \"%s\"", identifier));
    }

    @Override
    public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values)
            throws AlgorithmConfigurationException {
        switch (identifier) {
            case INPUT_FILE_CONFIG_KEY:
                if (values.length != 1) throw new AlgorithmConfigurationException("Only one input file supported.");
                this.inputGenerator = values[0];
                break;
            default:
                throw new IllegalArgumentException("Unsupported argument.");
        }
    }

    @Override
    public String getAuthors() {
        return "Sebastian Kruse";
    }

    @Override
    public String getDescription() {
        return "This is an implementation of the FDEP algorithm for partial FDs using the bottom-up approach.";
    }


    /**
     * Defines the configuration values for {@link FdepX}.
     */
    public static class Configuration extends AbstractPFDConfiguration {

        @MetanomeProperty
        private String tableIdentifier = null;

        @MetanomeProperty
        private boolean isCalculateErrors = false;

    }

    /**
     * Contains data on the execution performance in a {@link SearchSpace}.
     */
    public static class ProfilingData implements Serializable {
        public long initializationMillis = 0L;
        public long negativeCoverCalculationMillis = 0L;
        public long trimCoverMillis = 0L;
        public long coverInversionMillis = 0L;
        public long errorCalculationNanos = 0L;
        public long overallMillis = 0L;
        public long numDependencies = 0L;
        public long negativeCoverSize = 0L;
        public long trimmedNegativeCoverSize = 0L;

        public void printReport(String title, PrintStream out) {
            out.printf("=======================================================================================\n");
            out.printf("Report for %s\n", title);
            out.printf("---Phases------------------------------------------------------------------------------\n");
            out.printf("Initialization:                                                  %,10.3f s (%.2f%%)\n", initializationMillis / 1000d, getRuntimePercentage(initializationMillis));
            out.printf("Negative cover:                                                  %,10.3f s (%.2f%%)\n", negativeCoverCalculationMillis / 1000d, getRuntimePercentage(negativeCoverCalculationMillis));
            out.printf("Trim cover:                                                      %,10.3f s (%.2f%%)\n", trimCoverMillis / 1000d, getRuntimePercentage(trimCoverMillis));
            out.printf("Cover inversion:                                                 %,10.3f s (%.2f%%)\n", coverInversionMillis / 1000d, getRuntimePercentage(coverInversionMillis));
            out.printf("Error calculation:                                               %,10.3f s (%.2f%%)\n", errorCalculationNanos * 1e-9, getRuntimePercentage(errorCalculationNanos * 1e-6));
            out.printf("Total:                                                           %,10.3f s\n", overallMillis / 1000d);
            out.printf("---Miscellaneous-----------------------------------------------------------------------\n");
            out.printf("Dependencies:                                                    %,10d #\n", numDependencies);
            out.printf("Negative cover size:                                             %,10d\n", negativeCoverSize);
            out.printf("Trimmed negative cover size:                                     %,10d\n", trimmedNegativeCoverSize);
            out.printf("Error calculation efficiency:                                    %,10.3f ms/calculation\n", numDependencies / (double) errorCalculationNanos / 1e6);
            out.printf("=======================================================================================\n");
        }

        private double getRuntimePercentage(double millis) {
            return 100d * millis / overallMillis;
        }

    }

}
