package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.mdms.clients.MetacrateClient;
import de.hpi.isg.mdms.domain.constraints.PartialFunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.PartialUniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.pyro.core.AbstractPFDConfiguration;
import de.hpi.isg.pyro.core.DependencyConsumer;
import de.hpi.isg.pyro.model.*;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.properties.MetanomePropertyLedger;
import de.hpi.isg.pyro.tanex.FdErrorMeasure;
import de.hpi.isg.pyro.tanex.UccErrorMeasure;
import de.hpi.isg.pyro.util.LatticeLevel;
import de.hpi.isg.pyro.util.LatticeVertex;
import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;

import java.util.*;

/**
 * This TANE implementation specifically looks for approximate/partial FDs. However, we do not hardwire
 * g3 as error measure.
 * <p>Changes to the original algorithm:
 * <ul>
 * <li>search for keys (can cause a larger search space);</li>
 * <li>more error measures;</li>
 * <li>remove key vertices only after all key vertices on a level have been checked;</li>
 * <li>keep "invalid" {@link LatticeVertex}es (see {@link LatticeVertex#isInvalid});</li>
 * <li>keep singleton keys as RHS candidate, when looking for partial FDs.</li>
 * </ul></p>
 */
public class TaneX
        extends DependencyConsumer
        implements FunctionalDependencyAlgorithm, UniqueColumnCombinationsAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, FileInputParameterAlgorithm, BooleanParameterAlgorithm,
        MetacrateClient {

    /**
     * Defines the configuration values for {@link TaneX}.
     */
    public static class Configuration extends AbstractPFDConfiguration {

        @MetanomeProperty
        private String tableIdentifier = null;

    }

    public static final String INPUT_FILE_CONFIG_KEY = "inputFile";

    private FileInputGenerator fileInputGenerator;

    private MetanomePropertyLedger propertyLedger;
    private final Configuration configuration = new Configuration();

    private MetadataStore metadataStore;
    private Table table;
    private ConstraintCollection<PartialFunctionalDependency> pfdConstraintcollection;
    private ConstraintCollection<PartialUniqueColumnCombination> puccConstraintcollection;


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Profiling data.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public long _aprioriMillis = 0L;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Execution logic.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void execute() throws AlgorithmExecutionException {
        // Initialize.
        final FdErrorMeasure fdErrorMeasure;
        switch (this.configuration.fdErrorMeasure) {
            case "g1prime":
                fdErrorMeasure = FdErrorMeasure.g1Prime;
                break;
            default:
                throw new IllegalArgumentException(String.format("Illegal error measure: \"%s\"", this.configuration.fdErrorMeasure));
        }

        // Initialize.
        final UccErrorMeasure uccErrorMeasure;
        switch (this.configuration.uccErrorMeasure) {
            case "g1prime":
                uccErrorMeasure = UccErrorMeasure.g1Prime;
                break;
            default:
                throw new IllegalArgumentException(String.format("Illegal error measure: \"%s\"", this.configuration.uccErrorMeasure));
        }

        // Initialize operation on Metacrate.
        if (this.metadataStore != null) {
            if (this.configuration.tableIdentifier == null)
                throw new IllegalStateException("Cannot operate with Metacrate without a table.");
            this.table = this.metadataStore.getTableByName(this.configuration.tableIdentifier);
            if (this.table == null)
                throw new IllegalStateException("Invalid table identifier given.");
        }


        // Load data.
        final ColumnLayoutRelationData relation = ColumnLayoutRelationData.createFrom(
                this.fileInputGenerator, this.configuration.isNullEqualNull, this.configuration.maxCols, this.configuration.maxRows
        );
        RelationSchema schema = relation.getSchema();

        // Output info on the inspected relation.
        System.out.printf(
                "%s has %d columns, %d rows, and a maximum NIP of %.2f.\n",
                schema.getName(),
                relation.getNumColumns(),
                relation.getNumRows(),
                relation.getMaximumNip()
        );
        for (Column column : schema.getColumns()) {
            ColumnData columnData = relation.getColumnData(column.getIndex());
            double avgPartners = columnData.getPositionListIndex().getNep() * 2 / relation.getNumRows();
            System.out.printf("* %s: every tuple has %,.2f partners on average.\n", column, avgPartners);
        }

        // Create the first level.
        List<LatticeLevel> levels = new ArrayList<>();
        final LatticeLevel level0 = new LatticeLevel(0);
        LatticeVertex emptyVertex = new LatticeVertex(schema.emptyVertical);
        level0.add(emptyVertex);
        levels.add(level0); // pro forma

        // Find 0-ary FDs.
        BitSet zeroaryFdRhs = new BitSet();
        final LatticeLevel level1 = new LatticeLevel(1);
        for (Column column : schema.getColumns()) {
            ColumnData columnData = relation.getColumnData(column.getIndex());
            LatticeVertex vertex = new LatticeVertex(column);
            vertex.addRhsCandidates(schema.getColumns());
            vertex.getParents().add(emptyVertex);
            vertex.setKeyCandidate(true);
            vertex.setPositionListIndex(columnData.getPositionListIndex());
            level1.add(vertex);

            // Check if column forms 0-ary FD.
            double fdError = fdErrorMeasure.calculateZeroAryFdError(columnData, relation);
            if (fdError <= this.configuration.maxFdError) {
                zeroaryFdRhs.set(column.getIndex());
                this.registerFd(schema.emptyVertical, column, fdError, Double.NaN);
                vertex.getRhsCandidates().clear(column.getIndex());
                if (fdError == 0) {
                    vertex.getRhsCandidates().clear();
                }
            }
        }

        for (LatticeVertex vertex : level1.getVertices().values()) {
            Column column = (Column) vertex.getVertical();
            vertex.getRhsCandidates().andNot(zeroaryFdRhs);

            // Check if column forms a UCC.
            ColumnData columnData = relation.getColumnData(column.getIndex());
            double uccError = uccErrorMeasure.calculateUccError(columnData.getPositionListIndex(), relation);
            if (uccError <= this.configuration.maxUccError) {
                this.registerUcc(column, uccError, Double.NaN);
                vertex.setKeyCandidate(false);
                if (uccError == 0d) {
                    for (int rhsIndex = vertex.getRhsCandidates().nextSetBit(0);
                         rhsIndex != -1;
                         rhsIndex = vertex.getRhsCandidates().nextSetBit(rhsIndex + 1)) {
                        if (rhsIndex != column.getIndex()) {
                            this.registerFd(column, schema.getColumn(rhsIndex), 0d, Double.NaN);
                        }
                    }
                    vertex.getRhsCandidates().and(column.getColumnIndices());
                    // We invalidate the node if we are looking for exact dependencies, because then we will discover
                    // any remaining FDs towards this column via key-pruning.
                    if (this.configuration.maxFdError == 0 && this.configuration.maxUccError == 0) {
                        vertex.setInvalid(true);
                    }
                }
            }
        }
        levels.add(level1);

        for (int arity = 2; arity <= this.configuration.maxArity || this.configuration.maxArity <= 0; arity++) {
            long _startMillis = System.currentTimeMillis();
            LatticeLevel.clearLevelsBelow(levels, arity - 1);
            LatticeLevel.generateNextLevel(levels);
            _aprioriMillis += System.currentTimeMillis() - _startMillis;

            final LatticeLevel level = levels.get(arity);
            System.out.printf("Checking %,d %d-ary lattice vertices.\n", level.getVertices().size(), arity);
            if (level.getVertices().isEmpty()) break;

            // Compute dependencies.
            // We call the vertex "XA", as it is thought to comprise the verticals X and A.
            // Now, let's check all relevant FDs X -> A and keys XA.
            for (LatticeVertex xaVertex : level.getVertices().values()) {
                if (xaVertex.isInvalid()) continue;

                final ColumnCombination xa = ((ColumnCombination) xaVertex.getVertical());
                if (xaVertex.getPositionListIndex() == null) {
                    PositionListIndex parentPLI1 = xaVertex.getParents().get(0).getPositionListIndex();
                    PositionListIndex parentPLI2 = xaVertex.getParents().get(1).getPositionListIndex();
                    xaVertex.setPositionListIndex(parentPLI1.intersect(parentPLI2));
                }

                // Check for FDs.
                final BitSet xaIndices = xa.getColumnIndices();
                final BitSet aCandidates = xaVertex.getRhsCandidates();

                // For every RHS candidate...
                for (LatticeVertex xVertex : xaVertex.getParents()) {
                    final Vertical lhs = xVertex.getVertical();

                    // Find the column index that is not covered by X.
                    int aIndex = xaIndices.nextSetBit(0);
                    final BitSet xIndices = lhs.getColumnIndices();
                    while (xIndices.get(aIndex)) {
                        aIndex = xaIndices.nextSetBit(aIndex + 1);
                    }
                    if (!aCandidates.get(aIndex)) continue;

                    double error = fdErrorMeasure.calculateFdError(
                            xVertex.getPositionListIndex(),
                            xaVertex.getPositionListIndex(),
                            relation
                    );
                    if (error <= this.configuration.maxFdError) {
                        final Column rhs = schema.getColumns().get(aIndex);
                        this.registerFd(lhs, rhs, error, Double.NaN);
                        xaVertex.getRhsCandidates().clear(rhs.getIndex());
                        if (error == 0) {
                            xaVertex.getRhsCandidates().and(lhs.getColumnIndices());
                        }
                    }
                }
            }

            // Prune.
            Collection<LatticeVertex> keyVertices = new LinkedList<>();
            for (LatticeVertex vertex : level.getVertices().values()) {
                final ColumnCombination columns = ((ColumnCombination) vertex.getVertical());

                // But first, check for keys.
                if (vertex.isKeyCandidate()) {
                    double uccError = uccErrorMeasure.calculateUccError(vertex.getPositionListIndex(), relation);
                    if (uccError <= this.configuration.maxUccError) {
                        this.registerUcc(columns, uccError, Double.NaN);
                        vertex.setKeyCandidate(false);
                        if (uccError == 0d) {
                            for (int rhsIndex = vertex.getRhsCandidates().nextSetBit(0);
                                 rhsIndex != -1;
                                 rhsIndex = vertex.getRhsCandidates().nextSetBit(rhsIndex + 1)) {
                                Column rhs = schema.getColumn(rhsIndex);
                                if (!columns.contains(rhs)) {
                                    boolean isRhsCandidate = true;
                                    for (Column column : columns.getColumns()) {
                                        Vertical sibling = columns.without(column).union(rhs);
                                        LatticeVertex siblingVertex = level.getLatticeVertex(sibling.getColumnIndices());
                                        if (siblingVertex == null || !siblingVertex.getRhsCandidates().get(rhs.getIndex())) {
                                            isRhsCandidate = false;
                                            break;
                                        }
                                    }
                                    if (isRhsCandidate) this.registerFd(columns, rhs, 0d, Double.NaN);
                                }
                            }
                            keyVertices.add(vertex);
                        }
                    }
                }
            }
            // We invalidate the node if we are looking for exact dependencies, because then we will discover
            // any remaining FDs towards this column via key-pruning.
            if (this.configuration.maxFdError == 0 && this.configuration.maxUccError == 0) {
                // We need to lazily prune at the key vertices as we might otherwise lose FDs in sibling key vertices.
                for (LatticeVertex keyVertex : keyVertices) {
                    keyVertex.getRhsCandidates().and(keyVertex.getVertical().getColumnIndices());
                    keyVertex.setInvalid(true);
                }
            }
        }

        this.printProfilingData(relation);
    }


    private void printProfilingData(RelationData relation) {
        System.out.println("Collected profiling data:");
        System.out.printf(" Load data:            %,12d ms\n", relation._loadMillis);
        System.out.printf(" PLI intersects:       %,12d ms\n", PositionListIndex._nanosIntersects.get() / 1_000_000L);
        System.out.printf("                       %,12d [#]\n", PositionListIndex._numIntersects.get());
        System.out.printf(" Candidate generation: %,12d ms\n", _aprioriMillis);
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
    public void setFileInputConfigurationValue(String identifier,
                                               FileInputGenerator... values)
            throws AlgorithmConfigurationException {
        switch (identifier) {
            case INPUT_FILE_CONFIG_KEY:
                if (values.length != 1) throw new AlgorithmConfigurationException("Only one input file supported.");
                this.fileInputGenerator = values[0];
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
        return "Prototype to detect meaningful (partial) functional dependencies.";
    }


}
