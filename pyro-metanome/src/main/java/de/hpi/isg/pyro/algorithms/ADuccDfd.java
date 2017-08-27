package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.mdms.clients.MetacrateClient;
import de.hpi.isg.mdms.domain.constraints.PartialFunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.PartialUniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.pyro.core.AbstractPFDConfiguration;
import de.hpi.isg.pyro.core.DependencyConsumer;
import de.hpi.isg.pyro.ducc_dfd.FdGraphTraverser;
import de.hpi.isg.pyro.ducc_dfd.PliRepository;
import de.hpi.isg.pyro.ducc_dfd.UccGraphTraverser;
import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnData;
import de.hpi.isg.pyro.model.ColumnLayoutRelationData;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.properties.MetanomePropertyLedger;
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

import java.util.ArrayList;
import java.util.Date;

/**
 * This is a combined implementation of the Ducc and Dfd algorithm based on existing Metanome implementations.
 * Both algorithms are configured to discover approximate/partial FDs/UCCs.
 *
 * @author Sebastian Kruse
 */
public class ADuccDfd
        extends DependencyConsumer
        implements FunctionalDependencyAlgorithm, UniqueColumnCombinationsAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, FileInputParameterAlgorithm, BooleanParameterAlgorithm,
        MetacrateClient {

    public static final String INPUT_FILE_CONFIG_KEY = "inputFile";

    private FileInputGenerator fileInputGenerator;

    private MetanomePropertyLedger propertyLedger;
    private final ADuccDfd.Configuration configuration = new ADuccDfd.Configuration();

    private MetadataStore metadataStore;
    private Table table;
    private ConstraintCollection<PartialFunctionalDependency> pfdConstraintcollection;
    private ConstraintCollection<PartialUniqueColumnCombination> puccConstraintcollection;

    @Override
    public void execute() throws AlgorithmExecutionException {
        if (!this.configuration.isFindFds && !this.configuration.isFindKeys) {
            throw new AlgorithmExecutionException("Told to find neither FDs nor UCCs.");
        }

        System.out.println("Loading and preprocessing input data...");
        // Load the relation.
        ColumnLayoutRelationData relation = ColumnLayoutRelationData.createFrom(
                this.fileInputGenerator, this.configuration.isNullEqualNull, this.configuration.maxCols, this.configuration.maxRows
        );
        PliRepository pliRepository = new PliRepository(relation, this.configuration.pliCacheCapacity, this.configuration.protectedLruPlis);

        // Run A-DUCC.
        if (this.configuration.isFindKeys) {
            Vertical keyColumns = relation.getSchema().emptyVertical;
            long maxEqualityPairs = (long) (this.configuration.maxUccError * relation.getNumTuplePairs());
            for (ColumnData columnData : relation.getColumnData()) {
                double nep = columnData.getPositionListIndex().getNep();
                if (nep <= maxEqualityPairs) {
                    if (this.configuration.isFindKeys) {
                        this.registerUcc(columnData.getColumn(), nep / relation.getNumTuplePairs(), Double.NaN);
                    }
                    keyColumns = keyColumns.union(columnData.getColumn());
                }
            }

            System.out.println("Searching for UCCs...");
            UccGraphTraverser uccGraphTraverser = new UccGraphTraverser(relation.getSchema(),
                    pliRepository,
                    (lhs, error) -> this.registerUcc(lhs, error, Double.NaN),
                    keyColumns,
                    this.configuration.indexRebalancingThreshold,
                    this.configuration.maxUccError,
                    relation.getNumTuplePairs()
            );
            uccGraphTraverser.traverseGraph();
        }

        // Run A-DFD.
        if (this.configuration.isFindKeys) {
            for (Column rhs : relation.getSchema().getColumns()) {
                System.out.printf("Searching for FDs with %s as RHS...\n", rhs);

                // Run the actual algorithm.
                FdGraphTraverser fdGraphTraverser = new FdGraphTraverser(
                        rhs,
                        relation.getSchema(),
                        pliRepository,
                        (lhs, error) -> this.registerFd(lhs, rhs, error, Double.NaN),
                        rhs,
                        this.configuration.indexRebalancingThreshold,
                        this.configuration.maxUccError,
                        relation.getNumTuplePairs()
                );
                fdGraphTraverser.traverseGraph();
            }
        }

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
        return "This is an implementation of the FDEP algorithm for partial FDs using the bottom-up approach.";
    }


    /**
     * Defines the configuration values for {@link ADuccDfd}.
     */
    public static class Configuration extends AbstractPFDConfiguration {

        @MetanomeProperty
        private String tableIdentifier = null;

        @MetanomeProperty
        private int pliCacheCapacity = 10_000;

        @MetanomeProperty
        private int protectedLruPlis = 100;

        @MetanomeProperty
        private int indexRebalancingThreshold = 100_000;

    }

}
