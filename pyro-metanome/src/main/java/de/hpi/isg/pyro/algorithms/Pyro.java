package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.mdms.clients.MetacrateClient;
import de.hpi.isg.mdms.domain.constraints.PartialFunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.PartialUniqueColumnCombination;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.pyro.core.*;
import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnLayoutRelationData;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.properties.MetanomePropertyLedger;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.*;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementFileInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * This is an implementation of the Pyro algorithm without distributed computations.
 *
 * @author Sebastian Kruse
 */
public class Pyro
        extends DependencyConsumer
        implements FunctionalDependencyAlgorithm, UniqueColumnCombinationsAlgorithm,
        StringParameterAlgorithm, IntegerParameterAlgorithm, RelationalInputParameterAlgorithm, BooleanParameterAlgorithm,
        MetacrateClient {

    public static final String INPUT_FILE_CONFIG_KEY = "inputFile";

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private RelationalInputGenerator fileInputGenerator;

    private MetanomePropertyLedger propertyLedger;
    private final Configuration configuration = new Configuration();

    private MetadataStore metadataStore;
    private Table table;
    private ConstraintCollection<PartialFunctionalDependency> pfdConstraintcollection;
    private ConstraintCollection<PartialUniqueColumnCombination> puccConstraintcollection;

    @Override
    public void execute() throws AlgorithmExecutionException {
        final long initializationStartMillis = System.currentTimeMillis();
        if (!this.configuration.isFindFds && !this.configuration.isFindKeys) {
            throw new AlgorithmExecutionException("Told to find neither FDs nor UCCs.");
        }
        this.logger.info("Loading relation...");
        ColumnLayoutRelationData relationData = ColumnLayoutRelationData.createFrom(
                this.fileInputGenerator,
                configuration.isNullEqualNull,
                configuration.maxCols,
                configuration.maxRows
        );
        RelationSchema schema = relationData.getSchema();

        int parallelism = this.configuration.parallelism > 0 ?
                Math.min(this.configuration.parallelism, Runtime.getRuntime().availableProcessors()) :
                Runtime.getRuntime().availableProcessors();
        System.out.printf("Starting fixed thread pool with %d threads.\n", parallelism);
        ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

        // Prepare the profiling:
        // Profiling context.
        ProfilingContext profilingContext = new ProfilingContext(
                this.configuration,
                relationData,
                this.uccConsumer,
                this.fdConsumer
        );
        profilingContext.createColumnAgreeSetSamples(executorService::submit);

        // Launchpad order.
        Comparator<DependencyCandidate> launchpadOrder;
        switch (this.configuration.launchpadOrder) {
            case "arity":
                launchpadOrder = DependencyCandidate.fullArityErrorComparator;
                break;
            case "error":
                launchpadOrder = DependencyCandidate.fullErrorArityComparator;
                break;
            default:
                throw new AlgorithmExecutionException("Unknown comparator type.");
        }

        // Search spaces.
        Object2IntOpenHashMap<SearchSpace> searchSpaceCounters = new Object2IntOpenHashMap<>();
        int nextId = 0;
        if (configuration.isFindKeys) {
            DependencyStrategy strategy;
            switch (configuration.uccErrorMeasure) {
                case "g1prime":
                    strategy = new KeyG1Strategy(
                            configuration.maxUccError,
                            configuration.errorDev
                    );
                    break;
                default:
                    throw new AlgorithmExecutionException("Unknown key error measure.");

            }
            searchSpaceCounters.put(new SearchSpace(nextId++, strategy, relationData.getSchema(), launchpadOrder), 0);
        }
        if (configuration.isFindFds) {
            for (Column rhs : schema.getColumns()) {

                DependencyStrategy strategy;
                switch (configuration.uccErrorMeasure) {
                    case "g1prime":
                        strategy = new FdG1Strategy(
                                rhs,
                                configuration.maxUccError,
                                configuration.errorDev
                        );
                        break;
                    default:
                        throw new AlgorithmExecutionException("Unknown key error measure.");

                }
                searchSpaceCounters.put(new SearchSpace(nextId++, strategy, relationData.getSchema(), launchpadOrder), 0);
            }
        }
        searchSpaceCounters.keySet().forEach(searchSpace -> {
            searchSpace.setContext(profilingContext);
            searchSpace.ensureInitialized();
        });
        profilingContext.profilingData.initializationMillis.addAndGet(System.currentTimeMillis() - initializationStartMillis);

        final long operationStartMillis = System.currentTimeMillis();
        try {
            Collection<Future<?>> futures = new LinkedList<>();

            // Start the worker threads and wait for their completion.
            for (int i = 0; i < parallelism; i++) {
                futures.add(executorService.submit(() -> runWorker(searchSpaceCounters)));
            }
            for (Future<?> future : futures) {
                future.get();
            }
            futures.clear();

        } catch (InterruptedException | ExecutionException e) {
            throw new AlgorithmExecutionException("Execution interrupted.", e);
        } finally {
            if (!executorService.isShutdown()) {
                System.out.println("Shutting down the thread pool.");
                executorService.shutdownNow();
            }
            profilingContext.profilingData.operationMillis.addAndGet(System.currentTimeMillis() - operationStartMillis);
            profilingContext.profilingData.printReport("Pyro (Metanome)", System.out);
        }
    }

    private static void runWorker(Object2IntOpenHashMap<SearchSpace> searchSpaceCounters) {
        // Optimization: We keep track of which search spaces we visited.
        Set<SearchSpace> visitedSearchSpaces = new HashSet<>();
        boolean isVerbose = true;
        while (true) {
            SearchSpace searchSpace = null;
            synchronized (searchSpaceCounters) {
                if (searchSpaceCounters.isEmpty()) {
                    System.out.printf("Thread %s stops working.\n", Thread.currentThread().getName());
                    return;
                }

                int numThreads = -1;
                for (Object2IntMap.Entry<SearchSpace> entry : searchSpaceCounters.object2IntEntrySet()) {
                    if (visitedSearchSpaces.contains(entry.getKey())) continue;
                    if (numThreads == -1 || entry.getIntValue() < numThreads) {
                        searchSpace = entry.getKey();
                        numThreads = entry.getIntValue();
                    }
                }

                if (searchSpace != null) {
                    searchSpaceCounters.addTo(searchSpace, 1);
                    if (numThreads == 0) {
                        System.out.printf("Thread %s started working on %s.\n", Thread.currentThread().getName(), searchSpace);
                    } else if (isVerbose) {
                        System.out.printf("Thread %s joined on %s (%d+1).\n", Thread.currentThread().getName(), searchSpace, numThreads);
                    }
                }
            }

            if (searchSpace == null) {
                if (isVerbose) {
                    System.out.printf("Thread %s has worked on all search spaces and goes silent.\n", Thread.currentThread().getName());
                    isVerbose = false;
                }
                visitedSearchSpaces.clear();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // Pass.
                }
                continue;
            }

            visitedSearchSpaces.add(searchSpace);
            searchSpace.discover();

            synchronized (searchSpaceCounters) {
                int oldNumThreads = searchSpaceCounters.addTo(searchSpace, -1);
                if (isVerbose) {
                    System.out.printf("Thread %s left %s (%d-1).\n", Thread.currentThread().getName(), searchSpace, oldNumThreads);
                }
                if (oldNumThreads == 1) {
                    searchSpaceCounters.removeInt(searchSpace);
                    System.out.printf("%s has been removed.\n", searchSpace);
                }
            }
        }
    }

    @Override
    public void setResultReceiver(FunctionalDependencyResultReceiver resultReceiver) {
        if (this.metadataStore != null) return;

        this.fdConsumer = partialFD -> {
            try {
                synchronized (resultReceiver) {
                    resultReceiver.receiveResult(partialFD.toMetanomeFunctionalDependency());
                }
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
                synchronized (resultReceiver) {
                    resultReceiver.receiveResult(partialKey.toMetanomeUniqueColumnCobination());
                }
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
                if (values.length != 1) throw new AlgorithmConfigurationException(
                        String.format("Only one input file supported (%d given).", values.length)
                );
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
        return "Pyro uses a depth-first traversal strategy to find approximate UCCs and FDs. " +
                "This implementation uses Akka to distribute the different search paths among cores and/or among machines in a " +
                "cluster..";
    }


}
