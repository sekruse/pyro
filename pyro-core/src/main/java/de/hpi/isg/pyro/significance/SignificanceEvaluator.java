package de.hpi.isg.pyro.significance;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnLayoutRelation;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.properties.MetanomePropertyLedger;
import de.hpi.isg.pyro.significance.histograms.HistogramBuilder;
import de.hpi.isg.pyro.significance.histograms.HistogramDomain;
import de.hpi.isg.pyro.significance.metrics.EqualityPairOverlap;
import de.hpi.isg.pyro.util.ColumnCombinationIndex;
import de.hpi.isg.pyro.util.HyperGeometricDistributions;
import de.hpi.isg.pyro.util.PFDRater;
import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.FileInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.input.FileInputGenerator;

import java.util.*;
import java.util.concurrent.*;

/**
 * This app evaluates the correlation significance of column pairs.
 */
public class SignificanceEvaluator implements FileInputParameterAlgorithm, IntegerParameterAlgorithm, BooleanParameterAlgorithm {

    private MetanomePropertyLedger propertyLedger;

    @MetanomeProperty(name = "inputFile", required = true)
    private FileInputGenerator inputGenerator;

    @MetanomeProperty
    private int parallelism = 1;

    @MetanomeProperty
    private int numRepetitions = 100;

    @MetanomeProperty
    private int numBuckets = 100;

    @MetanomeProperty
    private boolean isCalculateVolatility = true;

    @MetanomeProperty
    private boolean isVerbose = false;

    private List<CorrelationMetric> metrics = Arrays.asList(new EqualityPairOverlap());

    @Override
    public void execute() throws AlgorithmExecutionException {
        // Load the relation.
        Relation relation = ColumnLayoutRelation.createFrom(this.inputGenerator, false);
        System.out.printf("Inspecting dataset with %,d columns and %,d rows.\n\n", relation.getNumColumns(), relation.getNumRows());

        this.runApproximateEvaluation(relation);

        if (this.numRepetitions > 0) {
            runEmpiricalEvaluation(relation);
        }
    }

    private void runApproximateEvaluation(Relation relation) {

        HistogramBuilder logLeftTailHistogramBuilder = new HistogramBuilder();
        HistogramBuilder leftTailHistogramBuilder = new HistogramBuilder();
        HistogramBuilder logRightTailHistogramBuilder = new HistogramBuilder();
        HistogramBuilder rightTailHistogramBuilder = new HistogramBuilder();

        ArrayList<Vertical.Metric<PositionListIndex.Volatility>> volatilities = new ArrayList<>();

        for (int index1 = 0; index1 < relation.getNumColumns(); index1++) {
            Column column1 = relation.getColumn(index1);
            for (int index2 = index1 + 1; index2 < relation.getNumColumns(); index2++) {
                Column column2 = relation.getColumn(index2);
                Vertical columnCombination = column1.union(column2);

                if (this.isCalculateVolatility) {
                    PositionListIndex.Volatility volatility = column1.getPositionListIndex().calculateVolatility(column2.getPositionListIndex(), false);
                    if (volatility.getChangeVectorLength() > 0) {
                        volatilities.add(new Vertical.Metric<>(columnCombination, volatility));
                    }
                }

                double value = new EqualityPairOverlap().evaluate(column1, column2, columnCombination, relation);
                double logLeftTailbound = PFDRater.logHyperGeometricConfidenceLT.rate(
                        column1, column2, columnCombination, relation
                );
                double leftTailbound = HyperGeometricDistributions.resolveLog(logLeftTailbound);
                double logRightTailbound = PFDRater.logHyperGeometricConfidenceGT.rate(
                        column1, column2, columnCombination, relation
                );
                double rightTailbound = HyperGeometricDistributions.resolveLog(logRightTailbound);

                logLeftTailHistogramBuilder.add(logLeftTailbound);
                leftTailHistogramBuilder.add(leftTailbound);
                logRightTailHistogramBuilder.add(logRightTailbound);
                rightTailHistogramBuilder.add(rightTailbound);
                if (this.isVerbose) {
                    System.out.printf("Looking at %s.\n", columnCombination);
                    System.out.printf(
                            "* %s: P(X <= %,.1f) = %s; P(X ≥ %,.1f) = %s\n",
                            columnCombination,
                            value, HyperGeometricDistributions.formatLogScientific(logLeftTailbound),
                            value, HyperGeometricDistributions.formatLogScientific(logRightTailbound)
                    );
                }
            }
        }

        if (!volatilities.isEmpty()) {
            System.out.printf("Volatilities:\n");
            volatilities.sort(Vertical.Metric.createComparator(
                    PositionListIndex.Volatility::getExpectedGain,
                    PositionListIndex.Volatility::getChangeVectorGainVectorSimilarity
            ));
            for (Vertical.Metric metric : volatilities) {
                System.out.printf("* %30s: %s.\n", metric.getVertical(), metric.getValue());
            }
            System.out.println();
        }

        // Print the histograms for the significance values.
        System.out.println("Approximation results:");
        {
            System.out.printf("* Equi-width histogram of significance values:\n");
            HistogramDomain equiwidthDomain = HistogramBuilder.buildEquiwidthDomain(this.numBuckets, leftTailHistogramBuilder, rightTailHistogramBuilder);
            equiwidthDomain.plotBucketTitlesAsCsv(System.out);
            leftTailHistogramBuilder.plotAgainstAsCsv(equiwidthDomain, System.out);
            rightTailHistogramBuilder.plotAgainstAsCsv(equiwidthDomain, System.out);
            System.out.println();

            System.out.printf("* Equi-depth histogram of significance values:\n");
            HistogramDomain equidepthDomain = HistogramBuilder.buildEquidepthDomain(this.numBuckets, leftTailHistogramBuilder, rightTailHistogramBuilder);
            equidepthDomain.plotBucketTitlesAsCsv(System.out);
            leftTailHistogramBuilder.plotAgainstAsCsv(equidepthDomain, System.out);
            rightTailHistogramBuilder.plotAgainstAsCsv(equidepthDomain, System.out);
            System.out.println();
        }

        // Print the histograms for the logarithmic significance values.
        {
            System.out.printf("* Equi-width histogram of logarithmic significance values:\n");
            HistogramDomain equiwidthDomain = HistogramBuilder.buildEquiwidthDomain(this.numBuckets, logLeftTailHistogramBuilder, logRightTailHistogramBuilder);
            equiwidthDomain.plotBucketTitlesAsCsv(System.out,
                    bound -> String.format("x < %s", HyperGeometricDistributions.formatLogScientific(bound)),
                    bounds -> String.format("%s <= x < %s", HyperGeometricDistributions.formatLogScientific(bounds[0]), HyperGeometricDistributions.formatLogScientific(bounds[1])),
                    bound -> String.format("x <= %s", HyperGeometricDistributions.formatLogScientific(bound))
            );
            logLeftTailHistogramBuilder.plotAgainstAsCsv(equiwidthDomain, System.out);
            logRightTailHistogramBuilder.plotAgainstAsCsv(equiwidthDomain, System.out);
            System.out.println();

            System.out.printf("* Equi-depth histogram of logarithmic significance values:\n");
            HistogramDomain equidepthDomain = HistogramBuilder.buildEquidepthDomain(this.numBuckets, logLeftTailHistogramBuilder, logRightTailHistogramBuilder);
            equidepthDomain.plotBucketTitlesAsCsv(System.out,
                    bound -> String.format("x < %s", HyperGeometricDistributions.formatLogScientific(bound)),
                    bounds -> String.format("%s <= x < %s", HyperGeometricDistributions.formatLogScientific(bounds[0]), HyperGeometricDistributions.formatLogScientific(bounds[1])),
                    bound -> String.format("%s <= x", HyperGeometricDistributions.formatLogScientific(bound))
            );
            logLeftTailHistogramBuilder.plotAgainstAsCsv(equidepthDomain, System.out);
            logRightTailHistogramBuilder.plotAgainstAsCsv(equidepthDomain, System.out);
            System.out.println();
        }

    }

    private void runEmpiricalEvaluation(Relation relation) {
        // Initialize the empirical raters.
        Collection<Future<ColumnCombinationIndex<List<EmpiricalSignificanceRater>>>> futureSignificanceRaters = new LinkedList<>();
        int parallelism = Math.min(this.parallelism, Runtime.getRuntime().availableProcessors());
        ExecutorService executorService = Executors.newFixedThreadPool(parallelism);
        int delta = this.numRepetitions / parallelism;
        for (int i = 0; i < parallelism; i++) {
            Relation actualRelation = i == parallelism - 1 ? relation : relation.copy();
            Future<ColumnCombinationIndex<List<EmpiricalSignificanceRater>>> future = executorService.submit(new EmpiricalEvaluationTask(
                    actualRelation, i * delta, (i + 1) * delta
            ));
            futureSignificanceRaters.add(future);
        }

        ColumnCombinationIndex<List<EmpiricalSignificanceRater>> empiricalSignificanceRaters = null;
        for (Future<ColumnCombinationIndex<List<EmpiricalSignificanceRater>>> futureSignificanceRater : futureSignificanceRaters) {
            try {
                ColumnCombinationIndex<List<EmpiricalSignificanceRater>> result = futureSignificanceRater.get();
                if (empiricalSignificanceRaters == null) {
                    empiricalSignificanceRaters = result;
                } else {
                    for (Map.Entry<BitSet, List<EmpiricalSignificanceRater>> entry : result.getIndex().entrySet()) {
                        List<EmpiricalSignificanceRater> existingRaters = empiricalSignificanceRaters.get(entry.getKey());
                        List<EmpiricalSignificanceRater> newRaters = entry.getValue();
                        for (int i = 0; i < existingRaters.size(); i++) {
                            EmpiricalSignificanceRater existingRater = existingRaters.get(i);
                            EmpiricalSignificanceRater newRater = newRaters.get(i);
                            existingRater.includeAll(newRater);
                        }
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        executorService.shutdownNow();

        // Output the results.
        System.out.printf("Empirical results:");
        HistogramBuilder leftTailHistogramBuilder = new HistogramBuilder();
        HistogramBuilder rightTailHistogramBuilder = new HistogramBuilder();
        for (int index1 = 0; index1 < relation.getNumColumns(); index1++) {
            Column column1 = relation.getColumn(index1);
            for (int index2 = index1 + 1; index2 < relation.getNumColumns(); index2++) {
                Column column2 = relation.getColumn(index2);
                Vertical columnCombination = column1.union(column2);
                System.out.printf("Results for %s:\n", columnCombination);

                List<EmpiricalSignificanceRater> localRaters = empiricalSignificanceRaters.get(columnCombination);
                for (EmpiricalSignificanceRater localRater : localRaters) {
                    double leftTailbound = localRater.getLeftTailbound();
                    double rightTailbound = localRater.getRightTailbound();
                    leftTailHistogramBuilder.add(leftTailbound);
                    rightTailHistogramBuilder.add(rightTailbound);
                    if (this.isVerbose) {
                        System.out.printf(
                                "* %s: P(X<=%,.1f) = %.3f; P(X>=%,.1f) = %.3f\n",
                                localRater.toString(),
                                localRater.getOriginalValue(), leftTailbound,
                                localRater.getOriginalValue(), rightTailbound
                        );
                    }
                }
            }
        }

        System.out.printf("* Equi-width histogram of significance values:\n");
        HistogramDomain equiwidthDomain = HistogramBuilder.buildEquiwidthDomain(this.numBuckets, leftTailHistogramBuilder, rightTailHistogramBuilder);
        equiwidthDomain.plotBucketTitlesAsCsv(System.out);
        leftTailHistogramBuilder.plotAgainstAsCsv(equiwidthDomain, System.out);
        rightTailHistogramBuilder.plotAgainstAsCsv(equiwidthDomain, System.out);
        System.out.println();

        System.out.printf("* Equi-depth histogram of significance values:\n");
        HistogramDomain equidepthDomain = HistogramBuilder.buildEquidepthDomain(this.numBuckets, leftTailHistogramBuilder, rightTailHistogramBuilder);
        equidepthDomain.plotBucketTitlesAsCsv(System.out);
        leftTailHistogramBuilder.plotAgainstAsCsv(equidepthDomain, System.out);
        rightTailHistogramBuilder.plotAgainstAsCsv(equidepthDomain, System.out);
        System.out.println();
    }

    private class EmpiricalEvaluationTask implements Callable<ColumnCombinationIndex<List<EmpiricalSignificanceRater>>> {

        private final Relation relation;

        private final int minRound, maxRound;

        private EmpiricalEvaluationTask(Relation relation, int minRound, int maxRound) {
            this.relation = relation;
            this.minRound = minRound;
            this.maxRound = maxRound;
        }

        @Override
        public ColumnCombinationIndex<List<EmpiricalSignificanceRater>> call() throws Exception {
            ColumnCombinationIndex<List<EmpiricalSignificanceRater>> empiricalSignificanceRaters = new ColumnCombinationIndex<>(relation);
            for (int index1 = 0; index1 < relation.getNumColumns(); index1++) {
                Column column1 = relation.getColumn(index1);
                for (int index2 = index1 + 1; index2 < relation.getNumColumns(); index2++) {
                    Column column2 = relation.getColumn(index2);
                    Vertical columnCombination = column1.union(column2);

                    List<EmpiricalSignificanceRater> localRaters = new ArrayList<>();
                    for (CorrelationMetric metric : metrics) {
                        localRaters.add(new EmpiricalSignificanceRater(metric, column1, column2, columnCombination, relation));
                    }
                    empiricalSignificanceRaters.put(columnCombination, localRaters);
                }
            }

            // Gather the empirical raters.
            for (int round = this.minRound; round < this.maxRound; round++) {
                // Shuffle.
                System.out.printf("Running empirical test round %d...\n", round);
                relation.shuffleColumns();

                // Update all the empirical significance raters.
                for (int index1 = 0; index1 < relation.getNumColumns(); index1++) {
                    Column column1 = relation.getColumn(index1);
                    for (int index2 = index1 + 1; index2 < relation.getNumColumns(); index2++) {
                        Column column2 = relation.getColumn(index2);
                        Vertical columnCombination = column1.union(column2);
                        List<EmpiricalSignificanceRater> localRaters = empiricalSignificanceRaters.get(columnCombination);
                        for (EmpiricalSignificanceRater localRater : localRaters) {
                            localRater.includeRandomSpecimen(column1, column2, columnCombination, relation);
                        }
                    }
                }
            }

            return empiricalSignificanceRaters;
        }
    }


    @Override
    public void setFileInputConfigurationValue(String identifier, FileInputGenerator... values) throws AlgorithmConfigurationException {
        if (!this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            throw new AlgorithmConfigurationException(String.format("Unknown configuration: \"%s\"", identifier));
        }
    }

    @Override
    public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
        if (!this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            throw new AlgorithmConfigurationException(String.format("Unknown configuration: \"%s\"", identifier));
        }
    }

    @Override
    public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
        if (!this.getPropertyLedger().configure(this, identifier, (Object[]) values)) {
            throw new AlgorithmConfigurationException(String.format("Unknown configuration: \"%s\"", identifier));
        }
    }

    @Override
    public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
        ArrayList<ConfigurationRequirement<?>> configurationRequirements = new ArrayList<>();
        this.getPropertyLedger().contributeConfigurationRequirements(configurationRequirements);
        return configurationRequirements;
    }

    @Override
    public String getAuthors() {
        return "Sebastian Kruse";
    }

    @Override
    public String getDescription() {
        return "Utility to capture the significance of relationships between columns.";
    }

    public MetanomePropertyLedger getPropertyLedger() {
        if (this.propertyLedger == null) {
            try {
                this.propertyLedger = MetanomePropertyLedger.createFor(this);
            } catch (AlgorithmConfigurationException e) {
                throw new RuntimeException(e);
            }
        }
        return this.propertyLedger;
    }
}
