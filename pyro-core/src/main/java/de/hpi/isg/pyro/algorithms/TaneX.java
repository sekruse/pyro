package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.mdms.clients.MetacrateClient;
import de.hpi.isg.mdms.domain.constraints.PartialFunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.PartialUniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.pyro.model.*;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.properties.MetanomePropertyLedger;
import de.hpi.isg.pyro.util.*;
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
import java.util.stream.Collectors;

/**
 * This TANE implementation specifically looks for approximate/partial FDs. However, we do not hardwire
 * {@link PFDRater#g3 g3} as error measure.
 * <p>Changes to the original algorithm:
 * <ul>
 * <li>search for keys (can cause a larger search space);</li>
 * <li>more error measures;</li>
 * <li>remove key vertices only after all key vertices on a level have been checked;</li>
 * <li>keep "invalid" {@link LatticeVertex}es (see {@link LatticeVertex#isInvalid});</li>
 * <li>keep singleton keys as RHS candidate, when looking for partial FDs.</li>
 * </ul></p>
 */
public class TaneX implements FunctionalDependencyAlgorithm, UniqueColumnCombinationsAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, FileInputParameterAlgorithm, BooleanParameterAlgorithm,
        MetacrateClient {

    public static final String INPUT_FILE_CONFIG_KEY = "inputFile";

    private FileInputGenerator fileInputGenerator;

    private MetadataStore metadataStore;
    private ConstraintCollection<PartialFunctionalDependency> pfdConstraintcollection;
    private FunctionalDependencyResultReceiver fdResultReceiver;
    private ConstraintCollection<PartialUniqueColumnCombination> puccConstraintcollection;
    private UniqueColumnCombinationResultReceiver uccResultReceiver;

    private Map<Column, List<PartialFD>> partialFDs;
    private List<PartialKey> partialKeys;

    private MetanomePropertyLedger propertyLedger;

    @MetanomeProperty
    private String outputFile = null;

    @MetanomeProperty
    private boolean isNullEqualNull = true;

    // Algorithm settings //
    @MetanomeProperty(name = "fdErrorMeasure")
    private String fdErrorMeasureName = "g1prime";
    private PFDRater fdErrorMeasure;

    @MetanomeProperty
    private double maxFdError = 0.05;


    @MetanomeProperty(name = "uccErrorMeasure")
    private String uccErrorMeasureName = "g1prime";
    private PFDRater uccErrorMeasure;

    @MetanomeProperty
    private double maxUccError = 0.05;

    @MetanomeProperty
    private int maxArity = -1;

    @MetanomeProperty
    private int maxCols = -1;

    @MetanomeProperty
    private int maxRows = -1;

    @MetanomeProperty
    private String tableIdentifier;
    private Table table;

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
        switch (this.fdErrorMeasureName) {
            case "g1prime":
                this.fdErrorMeasure = PFDRater.g1prime;
                break;
            case "g1":
                this.fdErrorMeasure = PFDRater.g1;
                break;
            case "G1":
                this.fdErrorMeasure = PFDRater.G1;
                break;
            case "g2":
                this.fdErrorMeasure = PFDRater.g2;
                break;
            case "G2":
                this.fdErrorMeasure = PFDRater.G2;
                break;
            case "g3":
                this.fdErrorMeasure = PFDRater.g3;
                break;
            case "G3":
                this.fdErrorMeasure = PFDRater.G3;
                break;
            case "pdep":
                this.fdErrorMeasure = (x, a, xa, r) -> 1 - PFDRater.pdep.rate(x, a, xa, r);
                break;
            case "tau":
                this.fdErrorMeasure = ((x, a, xa, r) -> 1 - PFDRater.tau.rate(x, a, xa, r));
                break;
            case "mu":
                this.fdErrorMeasure = ((x, a, xa, r) -> 1 - PFDRater.mu.rate(x, a, xa, r));
                break;
            default:
                throw new IllegalArgumentException(String.format("Illegal error measure: \"%s\"", this.fdErrorMeasureName));
        }

        // Initialize.
        switch (this.uccErrorMeasureName) {
            case "g1prime":
                this.uccErrorMeasure = (x, a, xa, r) -> PFDRater.round(x.getNep() / r.getNumTuplePairs());
                break;
            default:
                throw new IllegalArgumentException(String.format("Illegal error measure: \"%s\"", this.uccErrorMeasureName));
        }

        // Initialize operation on Metacrate.
        if (this.metadataStore != null) {
            if (this.tableIdentifier == null)
                throw new IllegalStateException("Cannot operate with Metacrate without a table.");
            this.table = this.metadataStore.getTableByName(this.tableIdentifier);
            if (this.table == null)
                throw new IllegalStateException("Invalid table identifier given.");
        }


        // Load data.
        this.partialFDs = new HashMap<>();
        this.partialKeys = new ArrayList<>();
        final Relation relation = ColumnLayoutRelation.createFrom(
                this.fileInputGenerator, this.isNullEqualNull, this.maxCols, this.maxRows
        );

        // Output info on the inspected relation.
        System.out.printf(
                "%s has %d columns, %d rows, and a maximum NIP of %.2f.\n",
                relation.getName(),
                relation.getColumns().size(),
                relation.getNumRows(),
                relation.getMaximumNip()
        );
        for (Column column : relation.getColumns()) {
            double avgPartners = column.getNep() * 2 / relation.getNumRows();
            System.out.printf("* %s: every tuple has %,.2f partners on average.\n", column, avgPartners);
        }

        // Create the first level.
        List<LatticeLevel> levels = new ArrayList<>();
        final LatticeLevel level0 = new LatticeLevel(0);
        LatticeVertex emptyVertex = new LatticeVertex(Vertical.emptyVertical(relation));
        level0.add(emptyVertex);
        levels.add(level0); // pro forma

        // Find 0-ary FDs.
        BitSet zeroaryFdRhs = new BitSet();
        final LatticeLevel level1 = new LatticeLevel(1);
        for (Column column : relation.getColumns()) {
            // TODO: Check if column forms a 0-ary FD.

            LatticeVertex vertex = new LatticeVertex(column);
            vertex.addRhsCandidates(relation.getColumns());
            vertex.getParents().add(emptyVertex);
            vertex.setKeyCandidate(true);
            level1.add(vertex);

            // Check if column forms 0-ary FD.
            double fdError;
            switch (this.fdErrorMeasureName) {
                case "g1prime":
                    fdError = column.getNip() / relation.getNumTuplePairs();
                    break;
                case "g1":
                case "g2":
                case "pdep":
                    fdError = column.getNip() / relation.getNumRows() / relation.getNumRows();
                    break;
                case "G1":
                case "G2":
                    fdError = column.getNip();
                    break;
                default:
                    System.out.printf("Warning: Cannot calculate 0-ary FD error for %s with %s.\n", column, this.fdErrorMeasureName);
                    fdError = Double.NaN;
                    break;
            }
            if (fdError <= this.maxFdError) {
                zeroaryFdRhs.set(column.getIndex());
                this.registerFd(relation.emptyVertical, column, fdError);
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
            double uccError = this.uccErrorMeasure.rate(column, null, null, relation);
            if (uccError <= this.maxUccError) {
                this.registerUcc(column, uccError);
                vertex.setKeyCandidate(false);
                if (uccError == 0d) {
                    for (int rhsIndex = vertex.getRhsCandidates().nextSetBit(0);
                         rhsIndex != -1;
                         rhsIndex = vertex.getRhsCandidates().nextSetBit(rhsIndex + 1)) {
                        if (rhsIndex != column.getIndex()) {
                            this.registerFd(column, relation.getColumn(rhsIndex), 0d);
                        }
                    }
                    vertex.getRhsCandidates().and(column.getColumnIndices());
                    // We invalidate the node if we are looking for exact dependencies, because then we will discover
                    // any remaining FDs towards this column via key-pruning.
                    if (this.maxFdError == 0 && this.maxUccError == 0) {
                        vertex.setInvalid(true);
                    }
                }
            }
        }
        levels.add(level1);

        for (int arity = 2; arity <= this.maxArity || this.maxArity <= 0; arity++) {
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
                if (xa.tryGetPositionListIndex() == null) {
                    PositionListIndex parentPLI1 = xaVertex.getParents().get(0).getVertical().getPositionListIndex();
                    PositionListIndex parentPLI2 = xaVertex.getParents().get(1).getVertical().getPositionListIndex();
                    xa.setAndLockPositionListIndex(parentPLI1.intersect(parentPLI2));
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

                    final Column rhs = relation.getColumns().get(aIndex);

                    double error = this.fdErrorMeasure.rate(lhs, rhs, xa, relation);
                    if (error <= this.maxFdError) {
                        this.registerFd(lhs, rhs, error);
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
                    double uccError = this.uccErrorMeasure.rate(columns, null, null, relation);
                    if (uccError <= this.maxUccError) {
                        this.registerUcc(columns, uccError);
                        vertex.setKeyCandidate(false);
                        if (uccError == 0d) {
                            for (int rhsIndex = vertex.getRhsCandidates().nextSetBit(0);
                                 rhsIndex != -1;
                                 rhsIndex = vertex.getRhsCandidates().nextSetBit(rhsIndex + 1)) {
                                Column rhs = relation.getColumn(rhsIndex);
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
                                    if (isRhsCandidate) this.registerFd(columns, rhs, 0d);
                                }
                            }
                            keyVertices.add(vertex);
                        }
                    }
                }
            }
            // We invalidate the node if we are looking for exact dependencies, because then we will discover
            // any remaining FDs towards this column via key-pruning.
            if (this.maxFdError == 0 && this.maxUccError == 0) {
                // We need to lazily prune at the key vertices as we might otherwise lose FDs in sibling key vertices.
                for (LatticeVertex keyVertex : keyVertices) {
                    keyVertex.getRhsCandidates().and(keyVertex.getVertical().getColumnIndices());
                    keyVertex.setInvalid(true);
                }
            }
        }

        this.printProfilingData(relation);
        this.saveResults();
    }



    private void printProfilingData(Relation relation) {
        System.out.println("Collected profiling data:");
        System.out.printf(" Load data:            %,12d ms\n", relation._loadMillis);
        System.out.printf(" PLI intersects:       %,12d ms\n", PositionListIndex._nanosIntersects.get() / 1_000_000L);
        System.out.printf("                       %,12d [#]\n", PositionListIndex._numIntersects.get());
        System.out.printf(" Candidate generation: %,12d ms\n", _aprioriMillis);
    }

    private void saveResults() {
        if (this.outputFile == null) return;

        System.out.printf("Writing results to %s.\n", this.outputFile);

        // Sort FDs by arity and RHS.
        List<PartialFD> sortedPartialFDs = this.partialFDs.values().stream()
                .flatMap(Collection::stream)
                .sorted(Comparator.comparing(PartialFD::getArity).thenComparing(fd -> fd.rhs.getIndex()))
                .collect(Collectors.toList());

        try (FDPersistence.Writer writer = FDPersistence.createWriter(this.outputFile)) {
            for (PartialFD partialFD : sortedPartialFDs) {
                writer.write(partialFD);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void registerFd(Vertical lhs, Column rhs, double fdError) {
//        System.out.printf("FD:  %s~>%s with error %.32f\n", lhs, rhs, fdError);
        PartialFD fd = new PartialFD(lhs, rhs, fdError);
        this.partialFDs.computeIfAbsent(rhs, key -> new ArrayList<>()).add(fd);

        // Feed the result receiver or metadata store.
        if (this.metadataStore != null) {
            if (this.pfdConstraintcollection == null) {
                this.pfdConstraintcollection = this.metadataStore.createConstraintCollection(
                        String.format("Partial FDs from %s (%s)", this.getClass().getSimpleName(), new Date()),
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
    }

    protected void registerUcc(Vertical vertical, double error) {
//        System.out.printf("UCC:  %s with error %.32f\n", vertical, error);
        PartialKey partialKey = new PartialKey(vertical, error);
        this.partialKeys.add(partialKey);

        // Feed the result receiver or metadata store.
        if (this.metadataStore != null) {
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

        } else {
            try {
                this.uccResultReceiver.receiveResult(partialKey.toMetanomeUniqueColumnCobination());
            } catch (CouldNotReceiveResultException | ColumnNameMismatchException e) {
                // Ignore but notify.
                e.printStackTrace();
            }
        }
    }


    @Override
    public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
        this.fdResultReceiver = resultReceiver;
    }

    @Override
    public void setResultReceiver(UniqueColumnCombinationResultReceiver resultReceiver) {
        this.uccResultReceiver = resultReceiver;
    }

    public MetanomePropertyLedger getPropertyLedger() {
        if (this.propertyLedger == null) {
            try {
                this.propertyLedger = MetanomePropertyLedger.createFor(this);
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

        if (this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format("Unknown string parameter: \"%s\"", identifier));
    }

    @Override
    public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
        if (this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            return;
        }

        throw new AlgorithmConfigurationException(String.format("Unknown Boolean parameter: \"%s\"", identifier));
    }


    @Override
    public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
        if (this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
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


    @Override
    public void setMetadataStore(MetadataStore metadataStore) {
        this.metadataStore = metadataStore;
    }

}
