package de.hpi.isg.pyro.evaluation;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import de.hpi.isg.mdms.clients.parameters.MetadataStoreParameters;
import de.hpi.isg.mdms.clients.util.MetadataStoreUtil;
import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;
import de.hpi.isg.mdms.domain.constraints.PartialFunctionalDependency;
import de.hpi.isg.mdms.domain.util.DependencyPrettyPrinter;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.constraints.ConstraintCollection;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleMap;
import it.unimi.dsi.fastutil.objects.Object2DoubleOpenHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This app allows to rate the normalized discounted cumulative gain of {@link PartialFunctionalDependency}s.
 */
public class NDCGRater {

    private final Parameters parameters;

    private MetadataStore metacrate;
    private DependencyPrettyPrinter prettyPrinter;

    private Int2ObjectMap<TableSample> tableSamples = new Int2ObjectOpenHashMap<>();

    private BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

    public NDCGRater(Parameters parameters) {
        this.parameters = parameters;
    }

    /**
     * Runs this app.
     */
    private void run() {
        // Connect to Metacrate.
        this.metacrate = null;
        try {
            this.metacrate = MetadataStoreUtil.loadMetadataStore(this.parameters.metadataStoreParameters);
            this.prettyPrinter = new DependencyPrettyPrinter(this.metacrate);

            if (this.parameters.fdConstraintCollectionId == null) {
                System.out.printf("Available partial FD constraint collections:\n");
                for (ConstraintCollection<PartialFunctionalDependency> constraintCollection :
                        this.metacrate.getConstraintCollectionByConstraintType(PartialFunctionalDependency.class)) {
                    System.out.printf("* %10d / %30s: %s (scope=%s)\n",
                            constraintCollection.getId(),
                            Optional.ofNullable(constraintCollection.getUserDefinedId()).orElse("n/a"),
                            constraintCollection.getDescription(),
                            constraintCollection.getScope().stream().map(Target::getName).collect(Collectors.joining(", "))
                    );
                }
                System.out.printf("Choose a constraint collection: ");
                try {
                    this.parameters.fdConstraintCollectionId = this.reader.readLine();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            // Get the partial FD constraint collection.
            ConstraintCollection<PartialFunctionalDependency> fdConstraintCollection =
                    this.getCheckedConstraintCollection(
                            metacrate,
                            this.parameters.fdConstraintCollectionId,
                            PartialFunctionalDependency.class
                    );

            // Get or create the ratings constraint collections.
            ConstraintCollection<FunctionalDependencyRating> ratingConstraintCollection = null;
            Object2DoubleMap<FunctionalDependency> fdRatings = new Object2DoubleOpenHashMap<>();
            fdRatings.defaultReturnValue(Double.NaN);
            if (this.parameters.ratingConstraintCollectionId == null) {
                if (fdConstraintCollection.getScope().size() == 1) {
                    Collection<ConstraintCollection<FunctionalDependencyRating>> constraintCollections =
                            this.metacrate.getConstraintCollectionByConstraintTypeAndScope(
                                    FunctionalDependencyRating.class,
                                    fdConstraintCollection.getScope().iterator().next()
                            );
                    if (!constraintCollections.isEmpty()) {
                        ratingConstraintCollection = constraintCollections.iterator().next();
                        for (FunctionalDependencyRating partialFunctionalDependency : ratingConstraintCollection.getConstraints()) {
                            fdRatings.put(
                                    partialFunctionalDependency.getFunctionalDependency(),
                                    partialFunctionalDependency.getRelevance()
                            );
                        }
                    }

                }
                if (ratingConstraintCollection == null && !this.parameters.isDryRun) {
                    ratingConstraintCollection = metacrate.createConstraintCollection(
                            "FD Ratings",
                            FunctionalDependencyRating.class,
                            fdConstraintCollection.getScope().toArray(new Target[0])
                    );
                }

            } else {
                ratingConstraintCollection = this.getCheckedConstraintCollection(
                        metacrate,
                        parameters.ratingConstraintCollectionId,
                        FunctionalDependencyRating.class
                );
                for (FunctionalDependencyRating partialFunctionalDependency : ratingConstraintCollection.getConstraints()) {
                    fdRatings.put(
                            partialFunctionalDependency.getFunctionalDependency(),
                            partialFunctionalDependency.getRelevance()
                    );
                }
            }

            // Load and sort all the partial FDs.
            List<PartialFunctionalDependency> fds = new ArrayList<>(fdConstraintCollection.getConstraints());
            fds.removeIf(fd -> Double.isNaN(fd.getScore()));
            fds.sort(Comparator.comparing(PartialFunctionalDependency::getScore).reversed());

            // Go through all the FDs and rate them.
            int rank = 1;
            double dcg = 0d;
            for (PartialFunctionalDependency pfd : fds) {
                if (this.parameters.limit > 0 && this.parameters.limit < rank) break;

                System.out.println();
                System.out.printf("Rank %d) %s\n", rank, this.prettyPrinter.prettyPrint(pfd));

                FunctionalDependency fd = new FunctionalDependency(pfd.getLhsColumnIds(), pfd.getRhsColumnId());

                // Try to obtain a known relevance.
                double relevance = fdRatings.getDouble(fd);
                if (Double.isNaN(relevance)) {
                    relevance = this.promptRelevance(pfd);
                    if (Double.isNaN(relevance)) {
                        break;
                    }
                    if (!this.parameters.isDryRun) {
                        ratingConstraintCollection.add(new FunctionalDependencyRating(
                                fd, relevance
                        ));
                    }
                }

                System.out.printf("* Effective relevance of %s: %.1f\n", this.prettyPrinter.prettyPrint(fd), relevance);

                double regularizer = Math.log1p(rank) / Math.log(2);
                dcg += relevance / regularizer;
                rank++;
            }

            System.out.printf("Calculated a DCG of %e for %,d elements.\n", dcg, rank - 1);

        } finally {
            if (metacrate != null) metacrate.close();
        }
    }

    private double promptRelevance(PartialFunctionalDependency pfd) {
        if (this.parameters.sampleSize > 0) {
            // Print the top k theories for the partial FD to support the assessment.
            System.out.printf("* Top k theories of %s:\n", this.prettyPrinter.prettyPrint(pfd));
            int tableId = this.metacrate.getIdUtils().getTableId(pfd.getRhsColumnId());
            TableSample tableSample = this.getOrCreateTableSample(tableId);
            FunctionalDependency fd = new FunctionalDependency(pfd.getLhsColumnIds(), pfd.getRhsColumnId());
            ArrayList<TableSample.TheoryCounter> topTheories = tableSample.getTopTheories(fd, this.parameters.numTheories);
            ListIterator<TableSample.TheoryCounter> theoryCounterListIterator = topTheories.listIterator(topTheories.size());
            while (theoryCounterListIterator.hasPrevious()) {
                TableSample.TheoryCounter theoryCounter = theoryCounterListIterator.previous();
                System.out.printf(" * %s\n", theoryCounter.format(fd, this.metacrate));
            }
        }

        System.out.printf("* Rate the relevance (0-2): ");
        String answer;
        try {
            answer = this.reader.readLine();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (answer == null || answer.trim().isEmpty()) return Double.NaN;
        return Double.parseDouble(answer) / 2d;
    }

    @SuppressWarnings("unchecked")
    private <T> ConstraintCollection<T> getCheckedConstraintCollection(
            MetadataStore metacrate, String id, Class<T> type) {
        ConstraintCollection<?> constraintCollection;
        try {
            constraintCollection = metacrate.getConstraintCollection(Integer.parseInt(id));
        } catch (NumberFormatException e) {
            constraintCollection = metacrate.getConstraintCollection(id);
        }
        if (constraintCollection == null || constraintCollection.getConstraintClass() != type)
            throw new IllegalArgumentException("Invalid constraint collection.");
        return (ConstraintCollection<T>) constraintCollection;
    }

    private TableSample getOrCreateTableSample(int tableId) {
        TableSample tableSample = this.tableSamples.get(tableId);
        if (tableSample == null) {
            try {
                tableSample = TableSample.load(this.metacrate, (Table) this.metacrate.getTargetById(tableId), this.parameters.sampleSize);
                this.tableSamples.put(tableId, tableSample);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return tableSample;
    }

    public static void main(String[] args) {
        Parameters parameters = new Parameters();
        new JCommander(parameters).parse(args);
        new NDCGRater(parameters).run();
    }

    /**
     * Parameters for the {@link NDCGRater}.
     */
    private static final class Parameters {

        @ParametersDelegate
        private final MetadataStoreParameters metadataStoreParameters = new MetadataStoreParameters();

        @Parameter(names = "--fds", description = "ID of a partial FD constraint collection to rate")
        private String fdConstraintCollectionId = null;

        @Parameter(names = "--ratings", description = "ID of constraint collection with ratings; if omitted then a new constraint collection will be created")
        private String ratingConstraintCollectionId = null;

        @Parameter(names = {"-n", "--dry-run"}, description = "do not store new ratings")
        private boolean isDryRun = false;

        @Parameter(names = "--limit", description = "the maximum number of FDs to consider")
        private int limit = -1;

        @Parameter(names = "--theories", description = "number of theories to describe FDs")
        private int numTheories = 20;

        @Parameter(names = "--sample", description = "data sample size to draw theories from")
        private int sampleSize = 10000;

    }
}
