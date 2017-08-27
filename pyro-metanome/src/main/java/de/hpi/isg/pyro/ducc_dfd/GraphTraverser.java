package de.hpi.isg.pyro.ducc_dfd;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Generic graph traversal implementation usable for both UCC and FD discovery.
 *
 * @author Jens Ehrlich
 * @author Jakob Zwiener
 * @author Mandy Roick
 * @author Lukas Schulze
 * @author Sebastian Kruse
 */
public abstract class GraphTraverser {

    /**
     * Calculates and caches {@link PositionListIndex}es.
     */
    protected final PliRepository pliRepository;

    /**
     * The {@link RelationSchema} that is being profiled.
     */
    private final RelationSchema schema;

    /**
     * {@link Column}s that should not be traversed.
     */
    protected final Vertical prunedColumns;

    /**
     * Keeps track of the non-dependencies.
     */
    protected final PruningGraph negativeGraph;

    /**
     * Keeps track of the dependencies.
     */
    protected final PruningGraph positiveGraph;

    /**
     * The minimal dependencies.
     */
    protected final Set<Vertical> minimalPositives = new HashSet<>();

    /**
     * The maximal non-dependencies.
     */
    protected final Set<Vertical> maximalNegatives = new HashSet<>();

    /**
     * The current seeds to start random walks from.
     */
    protected ArrayList<Vertical> seeds;

    /**
     * Keeps track of where there are holes in the attribute lattice.
     */
    protected final HoleFinder holeFinder;

    /**
     * Provides randomness for the random walk.
     */
    protected final Random random = new Random();

    /**
     * The number of discovered dependencies.
     */
    protected int found = 0;

    /**
     * Invoked on any {@link Vertical} that was identified to be a minimum dependency. The error is also passed.
     */
    protected final BiConsumer<Vertical, Double> minimumDependencyConsumer;

    /**
     * Creates a new instance.
     *
     * @param schema                        which should be traversed
     * @param pliRepository                 provides {@link PositionListIndex}es
     * @param minimumDependencyConsumer
     * @param pruningGraphPartitionCapacity see {@link PruningGraph#partitionCapacity}
     * @param prunedColumns                 that should not be traversed
     */
    protected GraphTraverser(RelationSchema schema,
                             PliRepository pliRepository,
                             BiConsumer<Vertical, Double> minimumDependencyConsumer,
                             int pruningGraphPartitionCapacity, Vertical prunedColumns) {
        this.schema = schema;
        this.pliRepository = pliRepository;
        this.minimumDependencyConsumer = minimumDependencyConsumer;
        this.prunedColumns = prunedColumns;
        this.positiveGraph = new PruningGraph(this.schema, PruningGraph.CoverType.SUBSETS, pruningGraphPartitionCapacity);
        this.negativeGraph = new PruningGraph(this.schema, PruningGraph.CoverType.SUPERSETS, pruningGraphPartitionCapacity);
        this.holeFinder = new HoleFinder(this.schema, prunedColumns);
        this.seeds = new ArrayList<>(Arrays.asList(this.prunedColumns.invert().getColumns()));
    }

    /**
     * Perform the actual traversal.
     *
     * @return the number of discovered dependencies
     */
    public int traverseGraph() throws CouldNotReceiveResultException, ColumnNameMismatchException {
        this.found = 0;
        //initial PLI
        Vertical seed = this.getSeed();
        while (null != seed) {
            this.randomWalk(seed);
            seed = this.getSeed();
        }

        return this.found;
    }

    /**
     * Perform a random walk in the dependency lattice starting at the given {@link Vertical}.
     *
     * @param seed from which the random walk is started
     */
    protected void randomWalk(Vertical seed)
            throws CouldNotReceiveResultException, ColumnNameMismatchException {

        System.out.printf("Processing seed %s...\n", seed);

        Stack<PathElement> trace = new Stack<>();
        Vertical nextVertical = seed;
        RandomWalk:
        while (true) {
            // If no vertical is given, we should follow the top of the trace stack.
            if (nextVertical == null) {
                while (!trace.isEmpty()) {
                    PathElement pathElement = trace.peek();
                    if ((nextVertical = this.pollRandomVertical(pathElement.destinations, false)) != null) {
                        continue RandomWalk;
                    } else {
                        // If we cannot follow any destination anymore, check whether the vertical is minimal/maximal.
                        if (pathElement.error <= this.getErrorThreshold()) {
                            Vertical coverElement = this.positiveGraph.getCoverElement(pathElement.vertical);
                            assert coverElement != null : String.format(
                                    "%s is an already visited dependency, but is not covered in the positive graph.",
                                    pathElement.vertical
                            );
                            if (coverElement.equals(pathElement.vertical)) {
                                this.found++;
                                this.minimalPositives.add(coverElement);
                                this.minimumDependencyConsumer.accept(coverElement, pathElement.error);
                            }
                        } else {
                            Vertical coverElement = this.negativeGraph.getCoverElement(pathElement.vertical);
                            if (coverElement.equals(pathElement.vertical)) {
                                this.maximalNegatives.add(coverElement);
                                this.holeFinder.updateMaximalNonDependency(coverElement);
                            }
                        }

                        trace.pop();
                    }
                }

                // If we cannot backtrack further, we are done.
                return;
            }

            // At first, check whether the next vertical is already classified.
            boolean isKnownDependency = this.positiveGraph.getCoverElement(nextVertical) != null;
            boolean isKnownNonDependency = !isKnownDependency && this.negativeGraph.getCoverElement(nextVertical) != null;
            if (isKnownDependency || isKnownNonDependency) {
                nextVertical = null;
                continue RandomWalk;
            }

            // Otherwise, test the vertical.
            PathElement newPathElement;
            double error = this.calculateError(nextVertical);
            if (error <= this.getErrorThreshold()) {
                newPathElement = new PathElement(nextVertical, this.getNonEmptyDirectSubsets(nextVertical), error);
                this.positiveGraph.add(nextVertical);

            } else {
                newPathElement = new PathElement(nextVertical, this.getDirectSupersets(nextVertical), error);
                this.negativeGraph.add(nextVertical);
            }
            trace.push(newPathElement);
            nextVertical = null;
        }

    }

    /**
     * Calculate the error of the dependency candidate entailed by the {@link Vertical}.
     * The error need only be correct if the {@link Vertical} entails a dependency. Otherwise, any value
     * above the error threshold will do.
     *
     * @param currentColumnCombination the {@link Vertical}
     * @return the dependency error
     */
    protected abstract double calculateError(Vertical currentColumnCombination);

    /**
     * Get the configured error threshold.
     *
     * @return the error threshold
     */
    protected abstract double getErrorThreshold();

    /**
     * Retrieve a random seed.
     *
     * @return the seed {@link Vertical} or {@code null} if none
     */
    protected Vertical getSeed() {
        Vertical seedCandidate = this.pollRandomVertical(this.seeds, false);
        if (seedCandidate == null) {
            this.seeds = this.getHoles();
            seedCandidate = this.pollRandomVertical(this.seeds, false);
        }

        return seedCandidate;
    }

    /**
     * Retrieve currently known holes.
     *
     * @return the holes
     */
    protected ArrayList<Vertical> getHoles() {
        return this.holeFinder.getHoles(this.minimalPositives);
    }

    /**
     * Retrieve all non-empty {@link Vertical}s that are direct subsets of the given {@link Vertical}.
     *
     * @param vertical the {@link Vertical}
     * @return the direct subset {@link Vertical}s
     */
    protected final ArrayList<Vertical> getNonEmptyDirectSubsets(Vertical vertical) {
        ArrayList<Vertical> subverticals = new ArrayList<>(vertical.getArity() - 1);
        if (vertical.getArity() == 1) return subverticals;
        BitSet columnIndices = vertical.getColumnIndices();
        for (int columnIndex = columnIndices.nextSetBit(0);
             columnIndex != -1;
             columnIndex = columnIndices.nextSetBit(columnIndex + 1)) {
            subverticals.add(vertical.without(this.schema.getColumn(columnIndex)));
        }
        return subverticals;
    }

    /**
     * Retrieve all {@link Vertical}s that are direct supersets of the given {@link Vertical} except for those
     * that contain an (additional) pruned {@link Column}.
     *
     * @param vertical the {@link Vertical}
     * @return the direct superset {@link Vertical}s
     */
    protected final ArrayList<Vertical> getDirectSupersets(Vertical vertical) {
        ArrayList<Vertical> superverticals = new ArrayList<>();
        BitSet columnIndices = vertical.getColumnIndices();
        for (int unusedColumnIndex = columnIndices.nextClearBit(0);
             unusedColumnIndex < this.schema.getNumColumns();
             unusedColumnIndex = columnIndices.nextClearBit(unusedColumnIndex + 1)) {
            if (this.prunedColumns.getColumnIndices().get(unusedColumnIndex)) continue;
            superverticals.add(vertical.union(this.schema.getColumn(unusedColumnIndex)));
        }
        return superverticals;
    }

    /**
     * Remove an (unpruned) (not a known (non-)dependency) {@link Vertical} from the given {@link ArrayList};
     *
     * @param verticals  the {@link Vertical}s to pick from
     * @param isUnpruned whether to ensure that the polled {@link Vertical} is not pruned
     * @return a random (unpruned) {@link Vertical} or {@code null} if none
     */
    protected Vertical pollRandomVertical(ArrayList<Vertical> verticals, boolean isUnpruned) {
        while (!verticals.isEmpty()) {
            // Remove a random vertical.
            int randomIndex = this.random.nextInt(verticals.size());
            Vertical randomVertical = verticals.get(randomIndex);
            verticals.set(randomIndex, verticals.get(verticals.size() - 1));
            verticals.remove(verticals.size() - 1);

            // Is the element a known (non-)dependency?
            if (isUnpruned && (this.positiveGraph.getCoverElement(randomVertical) != null
                    || this.negativeGraph.getCoverElement(randomVertical) != null)) {
                // Remove the element.
                continue;
            }

            return randomVertical;
        }
        return null;
    }

    public Collection<Vertical> getMinimalPositiveColumnCombinations() {
        return this.minimalPositives;
    }

    /**
     * Describes a visited {@link Vertical}.
     */
    private static final class PathElement {

        /**
         * The {@link Vertical} being visited.
         */
        final Vertical vertical;

        /**
         * Unvisited destinations.
         */
        final ArrayList<Vertical> destinations;

        /**
         * The calculated error of the dependency candidate.
         */
        final double error;

        private PathElement(Vertical vertical, ArrayList<Vertical> destinations, double error) {
            this.vertical = vertical;
            this.destinations = destinations;
            this.error = error;
        }
    }
}