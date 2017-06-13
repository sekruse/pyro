package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnLayoutRelation;
import de.hpi.isg.pyro.model.PartialKey;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.properties.MetanomeProperty;
import de.hpi.isg.pyro.util.*;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.lang3.Validate;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static de.hpi.isg.pyro.util.PositionListIndex.singletonValueId;

/**
 * Pyro is approximate key/FD discovery algorithm. Its search strategy resembles a skyrocket: First it ascends the
 * search space until it hits a dependency, then it trickles down the search space to discover minimal depedencies.
 * This is then repeated until the whole search space is covered.
 */
public class Pyro extends AbstractPFDAlgorithm {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Traversal settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    private int parallelism = 0;

    @MetanomeProperty
    private boolean isDeferFailedLaunchPads = true;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sampling settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    private int sampleSize = 1_000;

    @MetanomeProperty
    private double sampleBooster = 10;

    @MetanomeProperty
    private Integer seed = null;

    @MetanomeProperty
    private double estimateConfidence = 0.9;

    @MetanomeProperty
    private int randomAscendThreads = 2;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Cache settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    private boolean isCacheIntermediatePLIs = false;

    @MetanomeProperty
    private boolean isUseWeakReferencesForPlis = true;

    @MetanomeProperty
    private boolean isUseWeakReferencesForSamples = false;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscellaneous settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    private boolean isPauseInitially = false;

    @MetanomeProperty
    private boolean isCheckEstimates = false;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Work data.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private PLICache pliCache;
    private VerticalMap<Reference<AgreeSetSample>> agreeSetSamples;
    private Random random;
    private ProfilingData _profilingData = new Pyro.ProfilingData();
    private final Collection<AgreeSetSample> stickyAgreeSetSamples = new LinkedList<>();
    private Object2IntOpenHashMap<SearchSpace> searchSpaceCounters = new Object2IntOpenHashMap<>();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Methods.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void execute() throws AlgorithmExecutionException {
        // Load data.
        this.loadDatasetAsColumns();
        System.out.printf("The relation has %,d tuple pairs.\n", (long) this.relation.getMaximumNip());

        // Initialize relevant data structures.
        this.initialize();

        if (this.isPauseInitially) User.prompt("Press enter to continue.");

        int parallelism = this.parallelism > 0 ?
                Math.min(this.parallelism, Runtime.getRuntime().availableProcessors()) :
                Runtime.getRuntime().availableProcessors();
        System.out.printf("Starting fixed thread pool with %d threads.\n", parallelism);
        ExecutorService executorService = Executors.newFixedThreadPool(parallelism);

        try {
            Collection<Future<?>> futures = new LinkedList<>();

            if (this.sampleSize > 0) {
                // Create the initial samples.
                this.agreeSetSamples = parallelism > 1 ?
                        new SynchronizedVerticalMap<>(this.relation) :
                        new VerticalMap<>(this.relation);
                // Make sure to always have a cover of correlation providers (i.e., make the GC always spare the initial providers).
                for (Column column : this.relation.getColumns()) {
                    futures.add(executorService.submit(() -> {
                        AgreeSetSample sample = this.createFocusedSample(column, 1d, false);
                        synchronized (this.stickyAgreeSetSamples) {
                            this.stickyAgreeSetSamples.add(sample);
                        }
                    }));
                }
                for (Future<?> future : futures) {
                    future.get();
                }
                futures.clear();
            }

            // Consider each possible RHS.
            if (this.isFindKeys) {
                this.searchSpaceCounters.put(new SearchSpace(new KeyG1Strategy(this.maxKeyError)), 0);
            }

            if (this.isFindFds) {
                Validate.isTrue("g1prime".equals(this.fdErrorMeasure));
                for (Column column : this.relation.getColumns()) {
                    FdG1Strategy strategy = new FdG1Strategy(column, this.maxFdError);
                    // Catch for 0-ary FD.
                    double baseError = strategy.calculateError(this.relation.emptyVertical);
                    if (baseError <= this.maxFdError) {
                        strategy.registerDependency(this.relation.emptyVertical, baseError);
                    } else {
                        this.searchSpaceCounters.put(new SearchSpace(strategy), 0);
                    }
                }
            }

            // Start the worker threads and wait for their completion.
            for (int i = 0; i < parallelism; i++) {
                futures.add(executorService.submit(this::runWorker));
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
        }


        this.printSummary();
    }

    private void runWorker() {
        // Optimization: We keep track of which search spaces we visited.
        Set<SearchSpace> visitedSearchSpaces = new HashSet<>();
        boolean isVerbose = true;
        while (true) {
            SearchSpace searchSpace = null;
            synchronized (this.searchSpaceCounters) {
                if (this.searchSpaceCounters.isEmpty()) {
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
                    this.searchSpaceCounters.addTo(searchSpace, 1);
                    searchSpace.isAscendRandomly = this.randomAscendThreads > 0 && this.randomAscendThreads <= numThreads + 1;
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
            searchSpace.discover(null);

            synchronized (this.searchSpaceCounters) {
                int oldNumThreads = this.searchSpaceCounters.addTo(searchSpace, -1);
                if (isVerbose) {
                    System.out.printf("Thread %s left %s (%d-1).\n", Thread.currentThread().getName(), searchSpace, oldNumThreads);
                }
                searchSpace.isAscendRandomly = this.randomAscendThreads > 0 && this.randomAscendThreads <= oldNumThreads - 1;
                if (oldNumThreads == 1) {
                    this.searchSpaceCounters.removeInt(searchSpace);
                    System.out.printf("%s has been removed.\n", searchSpace);
                }
            }
        }
    }

    private AgreeSetSample createFocusedSample(Vertical focus, double boostFactor) {
        return this.createFocusedSample(focus, boostFactor, this.isUseWeakReferencesForSamples);
    }

    private AgreeSetSample createFocusedSample(Vertical focus, double boostFactor, boolean isUseWeakReference) {
        ListAgreeSetSample sample = ListAgreeSetSample.createFocusedFor(
                (ColumnLayoutRelation) this.relation,
                focus,
                this.pliCache.getPositionListIndex(focus),
                (int) (this.sampleSize * boostFactor),
                this.random
        );
        this.info("Created %s with a boost factor of %,f\n", sample, boostFactor);
        this.agreeSetSamples.put(focus, isUseWeakReference ? new WeakReference<>(sample) : new SoftReference<>(sample));
        return sample;
//        return TrieAgreeSetSample.createFocusedFor(
//                (ColumnLayoutRelation) this.relation,
//                focus,
//                focus.getPositionListIndex(),
//                this.sampleSize,
//                this.random
//        );
    }

    /**
     * Debug method to verify whether error estimates are correct. In particular, exact estimates must be correct.
     */
    private static void requireMinimalDependency(DependencyStrategy strategy, Vertical minDependency) {
        double error = strategy.calculateError(minDependency);
        if (error > strategy.maxError) {
            throw new AssertionError(String.format("%s should be a minimal dependency but has an error of %f.",
                    strategy.format(minDependency), error
            ));
        }
        if (minDependency.getArity() > 1) {
            for (Vertical parent : minDependency.getParents()) {
                double parentError = strategy.calculateError(parent);
                if (parentError <= strategy.maxError) {
                    throw new AssertionError(String.format("%s should be a minimal dependency but %s has an error of %f.",
                            strategy.format(minDependency), strategy.format(parent), error
                    ));
                }
            }
        }
    }


    private static boolean isSubsetPruned(Vertical lhs, VerticalMap<VerticalInfo> visitees) {
        for (Map.Entry<Vertical, VerticalInfo> subsetEntry : visitees.getSubsetEntries(lhs)) {
            if (subsetEntry.getValue().isPruningSupersets()) {
                return true;
            }
        }
        return false;
    }

    private static Collection<Vertical> getSubsetDeps(Vertical vertical, VerticalMap<VerticalInfo> verticalInfos) {
        return verticalInfos.getSubsetEntries(vertical).stream()
                .filter(entry -> entry.getValue().isDependency)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private static boolean isSupersetPruned(Vertical lhs, VerticalMap<VerticalInfo> visitees) {
        for (Map.Entry<Vertical, VerticalInfo> subsetEntry : visitees.getSupersetEntries(lhs)) {
            if (subsetEntry.getValue().isPruningSubsets()) {
                return true;
            }
        }
        return false;
    }

    private static boolean isKnownDependency(Vertical vertical, VerticalMap<VerticalInfo> verticalInfos) {
        for (Map.Entry<Vertical, VerticalInfo> entry : verticalInfos.getSubsetEntries(vertical)) {
            if (entry.getValue().isDependency) return true;
        }
        return false;
    }

    private static boolean isImpliedByMinDep(Vertical vertical, VerticalMap<VerticalInfo> verticalInfos) {
        for (Map.Entry<Vertical, VerticalInfo> entry : verticalInfos.getSubsetEntries(vertical)) {
            if (entry.getValue().isDependency && entry.getValue().isExtremal) return true;
        }
        return false;
    }

    private static boolean isKnownNonDependency(Vertical vertical, VerticalMap<VerticalInfo> verticalInfos) {
        for (Map.Entry<Vertical, VerticalInfo> entry : verticalInfos.getSupersetEntries(vertical)) {
            if (!entry.getValue().isDependency) return true;
        }
        return false;
    }

    /**
     * Whenever a LHS candidate is superset-pruned, we cannot simply discard the candidate. Instead, we need to move
     * the candidate out of the "pruning" shadow. Specifically, we need to supplement the LHS with minimum hitting sets
     * of the inversed {@code pruningSupersets}, because these ensure that we add a minimum amount of {@link Column}s
     * to the LHS candidates, such that these new candidates are not subset of any pruning superset any more.
     *
     * @param lhs                  that is pruned
     * @param strategy             for the dependency type being profiled for
     * @param pruningSupersets     that prune
     * @param lhsCandidates        accepts the novel candidates
     * @param visitedLhsCandidates from which to escape
     * @param seeds                existing launchPads; superset escapings should be discarded
     */
    private boolean escape(Vertical lhs,
                           DependencyStrategy strategy,
                           List<Vertical> pruningSupersets,
                           Collection<DependencyCandidate> lhsCandidates,
                           VerticalMap<VerticalInfo> visitedLhsCandidates,
                           VerticalMap<DependencyCandidate> seeds) {

        this.retainSupersetCover(pruningSupersets);

        // Special, yet regular case: there is only a single pruning superset.
        if (pruningSupersets.size() == 1) {
            Vertical pruningSuperset = pruningSupersets.get(0);
            for (int unprunedIndex = pruningSuperset.getColumnIndices().nextClearBit(0);
                 unprunedIndex < this.relation.getNumColumns();
                 unprunedIndex = pruningSuperset.getColumnIndices().nextClearBit(unprunedIndex + 1)) {
                if (strategy.isIrrelevantColumn(unprunedIndex)) continue;

                Column unprunedColumn = this.relation.getColumn(unprunedIndex);
                Vertical extendedLhs = lhs.union(unprunedColumn);

                // Check if we visited the extended LHS already.
                if (isSubsetPruned(extendedLhs, visitedLhsCandidates)) continue;

                if (!seeds.getSubsetEntries(extendedLhs).isEmpty()) {
//                    this.debug("Discarding non-minimal escaping %s.\n", extendedLhs);
                    continue;
                }

                // Now we need to estimate the FD error of this extended LHS.
                DependencyCandidate escapedLhsCandidate = strategy.createDependencyCandidate(extendedLhs);
//                        extendedLhsCandidate.error -= extendedLhs.getArity();
//                this.debug("* Adding seed %s.\n", escapedLhsCandidate);
                lhsCandidates.add(escapedLhsCandidate);

//                this.info("Escaped %s to %s.\n", lhs, extendedLhs);
            }
        } else {
            // Handle the large pruning supersets first: They might be in a subset relationship.
            pruningSupersets.sort(Comparator.comparing(Vertical::getArity).reversed());

            // Make sure, that the search space is not completely pruned, though.
            Vertical greatestPruningSuperset = pruningSupersets.get(0);
            if (greatestPruningSuperset.getArity() + strategy.getNumIrrelevantColumns() == this.relation.getNumColumns()) {
                return true;
            }

            // We keep track of the escaped LHS and refine them continuously.
            VerticalMap<Vertical> lhsEscapings = new VerticalMap<>(this.relation);

            // Apply the first pruning superset by just adding all unpruned columns as initially escaped LHS.
            Iterator<Vertical> pruningSupersetIterator = pruningSupersets.iterator();
            Vertical firstPruningSuperset = pruningSupersetIterator.next();
            for (int unprunedIndex = firstPruningSuperset.getColumnIndices().nextClearBit(0);
                 unprunedIndex < this.relation.getNumColumns();
                 unprunedIndex = firstPruningSuperset.getColumnIndices().nextClearBit(unprunedIndex + 1)) {
                if (strategy.isIrrelevantColumn(unprunedIndex)) continue;
                Column unprunedColumn = this.relation.getColumn(unprunedIndex);

                // We do early checking of subset-pruning so that we keep our intermediate results small.
                Vertical fullLhs = unprunedColumn.union(lhs);
                if (isSubsetPruned(fullLhs, visitedLhsCandidates)) continue;

                if (!seeds.getSubsetEntries(fullLhs).isEmpty()) {
//                    this.debug("Discarding non-minimal escaping %s.\n", fullLhs);
                    continue;
                }

                lhsEscapings.put(unprunedColumn, unprunedColumn);
            }

            // Now, continuously refine these escaped LHS.
            while (pruningSupersetIterator.hasNext()) {
                Vertical pruningSuperset = pruningSupersetIterator.next();
                ArrayList<Vertical> invalidLhsEscapes = lhsEscapings.getSubsetKeys(pruningSuperset);

                for (Vertical invalidLhsEscape : invalidLhsEscapes) {
                    for (int unprunedIndex = pruningSuperset.getColumnIndices().nextClearBit(0);
                         unprunedIndex < this.relation.getNumColumns();
                         unprunedIndex = pruningSuperset.getColumnIndices().nextClearBit(unprunedIndex + 1)) {
                        if (strategy.isIrrelevantColumn(unprunedIndex)) continue;

                        Column unprunedColumn = this.relation.getColumn(unprunedIndex);
                        Vertical correctedLhs = invalidLhsEscape.union(unprunedColumn);

                        // We do early checking of subset-pruning so that we keep our intermediate results small.
                        Vertical fullLhs = correctedLhs.union(lhs);
                        if (isSubsetPruned(fullLhs, visitedLhsCandidates)) continue;

                        if (!seeds.getSubsetEntries(fullLhs).isEmpty()) {
//                            this.debug("Discarding non-minimal escaping %s.\n", fullLhs);
                            continue;
                        }

                        lhsEscapings.put(correctedLhs, correctedLhs);
                    }
                    lhsEscapings.remove(invalidLhsEscape);
                }

                // Remove non-minimal escapings right away.
                for (Vertical lhsEscaping : new ArrayList<>(lhsEscapings.keySet())) {
                    if (lhsEscapings.getSubsetEntries(lhsEscaping).size() > 1) lhsEscapings.remove(lhsEscaping);
                }
            }

            // Finally, schedule all the escaped LHS.
            for (Vertical lhsEscaping : lhsEscapings.keySet()) {

                // Actually, we omitted the pruned part of the LHS so far. There is no point in including that,
                // because we know that it is contained in every pruning superset. This way, we can keep our
                // intermediate data structures smaller. Now, we add this part.
                Vertical escapedLhs = lhsEscaping.union(lhs);

                // Note that we don't need to check whether our escapings are subset-pruned because we checked
                // that already continuously during the refinement.

                // Now we need to estimate the FD error of this extended LHS.
                DependencyCandidate escapedLhsCandidate = strategy.createDependencyCandidate(escapedLhs);
//                this.debug("* Adding seed %s.\n", escapedLhsCandidate);
                lhsCandidates.add(escapedLhsCandidate);
                seeds.put(escapedLhs, escapedLhsCandidate);
            }

            return true;
        }

        return false;
    }

    /**
     * Retain only those {@link Vertical}s that do not have a superset-{@link Vertical}.
     *
     * @param family the {@link Vertical}s
     */
    private void retainSupersetCover(List<Vertical> family) {
        if (family == null || family.size() < 2) return;

        // Sort the verticals by their arity.
        family.sort(Comparator.comparing(Vertical::getArity).reversed());

        // Keep track of the cover verticals with a VerticalMap and remove any non-cover verticals.
        VerticalMap<Vertical> cover = new VerticalMap<>(this.relation);
        Iterator<Vertical> iterator = family.iterator();
        while (iterator.hasNext()) {
            Vertical vertical = iterator.next();
            if (!cover.getSupersetEntries(vertical).isEmpty()) {
                iterator.remove();
            } else {
                cover.put(vertical, vertical);
            }
        }
    }

    /**
     * Retrieve a {@link AgreeSetSample} from the {@link #agreeSetSamples} with a
     * best possible sampling ratio among all those that comprise the given {@code focus}.
     *
     * @param focus {@link Column}s that may be focused
     * @return the {@link AgreeSetSample}
     */
    private AgreeSetSample getAgreeSetSample(Vertical focus) {
        ArrayList<Map.Entry<Vertical, Reference<AgreeSetSample>>> correlationProviderEntries =
                this.agreeSetSamples.getSubsetEntries(focus);
        AgreeSetSample sample = null;
        for (Map.Entry<Vertical, Reference<AgreeSetSample>> correlationProviderEntry : correlationProviderEntries) {
            AgreeSetSample nextSample = correlationProviderEntry.getValue().get();

            if (sample == null || nextSample != null && nextSample.getSamplingRatio() > sample.getSamplingRatio()) {
                sample = nextSample;
            }
        }

        // It seems that the JVM might discard our SoftReferences even though we made the referencee strongly referenced.
        if (sample == null) {
            System.out.printf(
                    "Warning: Could not find any sample for %s. Sticky samples: %s. Found entries: %s.",
                    focus, this.stickyAgreeSetSamples, correlationProviderEntries
            );
            for (AgreeSetSample nextSample : this.stickyAgreeSetSamples) {
                if (!focus.contains(nextSample.getFocus())) continue;

                if (sample == null || nextSample.getSamplingRatio() > sample.getSamplingRatio()) {
                    sample = nextSample;
                }
            }
        }

        return sample;
    }

    /**
     * Candidate {@link Vertical}s to be traversed as described by the {@link #error}. They are ordered by
     * this {@link #error} ascendingly and by the arity of the {@link #vertical} descendingly.
     */
    private static class DependencyCandidate implements Comparable<DependencyCandidate> {

        public static Comparator<DependencyCandidate> arityComparator = (tc1, tc2) -> {
            // Primarily order by the arity.
            int result = Integer.compare(tc1.vertical.getArity(), tc2.vertical.getArity());
            if (result != 0) return result;

            // Use the error to break ties.
            return Double.compare(tc1.error.getMean(), tc2.error.getMean());
        };

        public static Comparator<DependencyCandidate> meanErrorComparator = (tc1, tc2) -> {
            // Primarily order by the error.
            int result = Double.compare(tc1.error.getMean(), tc2.error.getMean());
            if (result != 0) return result;

            // Use the arity to break ties.
            return Integer.compare(tc1.vertical.getArity(), tc2.vertical.getArity());
        };

        public static Comparator<DependencyCandidate> minErrorComparator = (tc1, tc2) -> {
            // Primarily order by the error.
            int result = Double.compare(tc1.error.getMin(), tc2.error.getMin());
            if (result != 0) return result;

            // Use the arity to break ties.
            return Integer.compare(tc1.vertical.getArity(), tc2.vertical.getArity());
        };

        /**
         * The {@link Vertical} to visit represented by this instance.
         */
        public final Vertical vertical;

        /**
         * An estimate of the key or FD error.
         */
        public ConfidenceInterval error;

        /**
         * The {@link AgreeSetSample} from which this instance has been created.
         */
        public AgreeSetSample agreeSetSample;

        public DependencyCandidate(Vertical vertical, ConfidenceInterval error, AgreeSetSample agreeSetSample) {
            this.vertical = vertical;
            this.error = error;
            this.agreeSetSample = agreeSetSample;
        }

        @Override
        public int compareTo(DependencyCandidate that) {
            // Primarily order by the error.
            int result = Double.compare(this.error.getMean(), that.error.getMean());
            if (result != 0) return result;

            // Use the arity to break ties.
            result = Integer.compare(that.vertical.getArity(), this.vertical.getArity());
            if (result != 0) return result;

            // Finally, apply a lexicographical comparison to remove duplicates.
            BitSet thisColumns = this.vertical.getColumnIndices();
            BitSet thatColumns = that.vertical.getColumnIndices();
            for (int a = thisColumns.nextSetBit(0), b = thatColumns.nextSetBit(0);
                 a != -1;
                 a = thisColumns.nextSetBit(a + 1), b = thatColumns.nextSetBit(b + 1)) {
                if (a < b) return -1;
                else if (a > b) return 1;
            }
            return 0;
        }

        @Override
        public String toString() {
            return String.format("candidate %s (err=%s, from %s)", this.vertical, this.error, this.agreeSetSample);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final DependencyCandidate that = (DependencyCandidate) o;
            return Objects.equals(vertical, that.vertical) &&
                    Objects.equals(error, that.error) &&
                    Objects.equals(agreeSetSample, that.agreeSetSample);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vertical, error, agreeSetSample);
        }

        public boolean isExact() {
            return this.error.isPoint() && (this.agreeSetSample == null || this.agreeSetSample.isExact());
        }
    }

    private static class VerticalInfo {

        boolean isDependency;
        boolean isExtremal;
        double error;

        static VerticalInfo forDependency() {
            return new VerticalInfo(true, false);
        }

        static VerticalInfo forMinimalDependency() {
            return new VerticalInfo(true, true);
        }

        static VerticalInfo forNonDependency() {
            return new VerticalInfo(false, false);
        }

        static VerticalInfo forMaximalNonDependency() {
            return new VerticalInfo(false, true);
        }


        VerticalInfo(boolean isDependency, boolean isExtremal) {
            this(isDependency, isExtremal, Double.NaN);
        }

        VerticalInfo(boolean isDependency, boolean isExtremal, double error) {
            this.isDependency = isDependency;
            this.isExtremal = isExtremal;
            this.error = error;
        }

        boolean isPruningSupersets() {
            return this.isDependency || this.isExtremal;
        }

        boolean isPruningSubsets() {
            return !this.isDependency || this.isExtremal;
        }

        @Override
        public String toString() {
            if (this.isDependency) {
                return this.isExtremal ? "minimal dependency" : "dependency";
            } else {
                return this.isExtremal ? "maximal non-dependency" : "non-dependency";
            }
        }
    }

    /**
     * Initialize relevant data structures.
     */
    void initialize() {
        super.initialize();

        if (!this.isFindOnlyMinimalPFDs || !this.isFindOnlyMinimalPKs) {
            throw new IllegalStateException("Can only find minimal dependencies.");
        }
        if (this.isFindRestrictedFDs) {
            throw new IllegalStateException("Restricted FDs currently not supported.");
        }
        if (this.isPruningWithPFDs) {
            throw new IllegalStateException("Pruning with PFDs (transitive rule) not supported.");
        }
        if (this.topKKeys > 0 || this.topKFds > 0) {
            System.out.printf("Warning: Top-k pruning might slow down the algorithm.");
        }

        this.pliCache = new PLICache(
                this.relation,
                this.parallelism > 1,
                this.isUseWeakReferencesForPlis ? WeakReference::new : SoftReference::new
        );
        this.pliCache.setCacheIntermediatePlis(this.isCacheIntermediatePLIs);

        this.random = this.seed != null ? new Random(this.seed) : new Random();
    }

    @Override
    void printSummary() {
        super.printSummary();

        System.out.printf("PLI cache size:      %,9d (including cache left-overs)\n", this.pliCache.size());
        System.out.printf("Sampling cache size: %,9d (including cache left-overs)\n", this.agreeSetSamples == null ? 0 : this.agreeSetSamples.size());
        System.out.println();
        System.out.printf("Load data:       %12d ms\n", this.relation._loadMillis);
        System.out.printf("PLI intersects:  %12d ms\n", PositionListIndex._nanosIntersects.get() / 1_000_000L);
        System.out.printf("                 %12d [#]\n", PositionListIndex._numIntersects.get());
        System.out.printf("PLI probings:    %12d ms\n", _profilingData.probingNanos.get() / 1_000_000L);
        System.out.printf("                 %12d [#]\n", _profilingData.numProbings.get());
        System.out.printf("Sampling:        %12d ms\n", AgreeSetSample._milliSampling.get());
        System.out.printf("                 %12d [#]\n", AgreeSetSample._numSamples.get());
        System.out.printf("Sample queries:  %12d ms\n", AgreeSetSample._nanoQueries.get() / 1_000_000L);
        System.out.printf("                 %12d [#]\n", AgreeSetSample._numQueries.get());
        System.out.printf("Hitting sets:    %12d ms\n", this.relation._hittingSetNanos.get() / 1_000_000L);
        System.out.printf("                 %12d [#]\n", this.relation._numHittingSets.get());
        System.out.printf("Ascend:          %12d ms\n", _profilingData.ascendMillis.get());
        System.out.printf("                 %12d [#]\n", _profilingData.numAscends.get());
        System.out.printf("Trickle down:    %12d ms\n", _profilingData.trickleDownMillis.get());
        System.out.printf("                 %12d [#]\n", _profilingData.numTrickleDowns.get());
    }

    @Override
    public String getAuthors() {
        return "Sebastian Kruse";
    }

    @Override
    public String getDescription() {
        return "Prototype to detect meaningful (partial) functional dependencies.";
    }

    /**
     * This class holds working data for a dependency search space.
     */
    private class SearchSpace {

        final DependencyStrategy strategy;

        final VerticalMap<VerticalInfo> globalVisitees;

        final SortedSet<DependencyCandidate> launchPads = new TreeSet<>();

        final VerticalMap<DependencyCandidate> launchPadIndex = new SynchronizedVerticalMap<>(Pyro.this.relation);

        final Lock launchPadIndexLock = new ReentrantLock();

        final LinkedList<DependencyCandidate> deferredLaunchPads = new LinkedList<>();

        final VerticalMap<Vertical> scope;

        final double sampleBoost;

        final int recursionDepth;

        boolean isAscendRandomly = false;

        /**
         * Create a new top-level instance.
         *
         * @param strategy defines the search within the new instance
         */
        SearchSpace(DependencyStrategy strategy) {
            this(strategy, null, new SynchronizedVerticalMap<>(Pyro.this.relation), 0, 1d);

            // Initialize the launchPads.
            for (Column column : Pyro.this.relation.getColumns()) {
                if (this.strategy.isIrrelevantColumn(column)) continue;

                // We need to estimate the error of the dependency candidate.
                this.addLaunchPad(this.strategy.createDependencyCandidate(column));
            }
        }

        /**
         * Create a new sub-level instance.
         *
         * @param strategy defines the search within the new instance
         */
        SearchSpace(DependencyStrategy strategy, VerticalMap<Vertical> scope, VerticalMap<VerticalInfo> globalVisitees, int recursionDepth, double sampleBoost) {
            this.strategy = strategy;
            this.scope = scope;
            this.globalVisitees = globalVisitees;
            this.recursionDepth = recursionDepth;
            this.sampleBoost = sampleBoost;
        }

        /**
         * This method discovers data dependencies (either keys or FDs).
         *
         * @param localVisitees known (non-)dependencies
         */
        private void discover(VerticalMap<VerticalInfo> localVisitees) {
            while (true) {
                DependencyCandidate launchPad = this.pollLaunchPad(localVisitees);
                if (launchPad == null) break;

                // Keep track of the visited dependency candidates to avoid duplicate visits and enable pruning.
                localVisitees = localVisitees != null ? localVisitees : new VerticalMap<>(Pyro.this.relation);
                boolean isDependencyFound = this.ascend(launchPad, localVisitees);

                this.returnLaunchPad(launchPad, !isDependencyFound);
            }
        }

        /**
         * Poll a launch pad from the {@link #launchPads}. This method takes care of synchronization issues and
         * escaping of the launch pads.
         *
         * @param localVisitees a complementary set of known {@link VerticalInfo}s to the {@link #globalVisitees}
         * @return the {@link DependencyCandidate} launch pad or {@code null} if none
         */
        DependencyCandidate pollLaunchPad(VerticalMap<VerticalInfo> localVisitees) {
            while (true) {
                final DependencyCandidate launchPad;
                synchronized (this.launchPads) {
                    if (this.launchPads.isEmpty()) {
                        if (this.deferredLaunchPads.isEmpty()) return null;

                        this.launchPads.addAll(this.deferredLaunchPads);
                        this.deferredLaunchPads.clear();
                    }

                    // Take the most promising launch pad.
                    Iterator<DependencyCandidate> launchPadIterator = this.launchPads.iterator();
                    launchPad = launchPadIterator.next();
                    launchPadIterator.remove();
                    this.launchPadIndex.remove(launchPad.vertical);
                }

                // TODO: Check if the launchPad can be estimated more accurately.

                // Make sure that the candidate is not subset-pruned, i.e., we already identified it to be a dependency.
                if (isImpliedByMinDep(launchPad.vertical, globalVisitees)
                        || (localVisitees != null && isImpliedByMinDep(launchPad.vertical, localVisitees))) {
                    // If it is subset-pruned, we can remove it.
                    debug("* Removing subset-pruned launch pad %s.\n", this.strategy.format(launchPad.vertical));
                    // Note: We can remove the launch pad without synchronization.
                    this.launchPadIndex.remove(launchPad.vertical);
                    continue;
                }

                // Make sure that the launchPad is not superset-pruned.
                ArrayList<Map.Entry<Vertical, VerticalInfo>> supersetEntries = this.globalVisitees.getSupersetEntries(launchPad.vertical);
                if (localVisitees != null) {
                    localVisitees.getSupersetEntries(launchPad.vertical).stream()
                            .filter(e -> e.getValue().isPruningSubsets())
                            .forEach(supersetEntries::add);
                }
                if (supersetEntries.isEmpty()) {
                    return launchPad;
                }
                try {
                    if (this.launchPadIndexLock.tryLock(100, TimeUnit.MILLISECONDS)) {
                        // If it is superset-pruned, escape the launchPad and start over.
                        if (isVerbose) {
                            debug("* Escaping launchPad %s from: %s\n",
                                    strategy.format(launchPad.vertical),
                                    supersetEntries.stream()
                                            .map(entry -> String.format("%s (%s)", strategy.format(entry.getKey()), entry.getValue()))
                                            .collect(Collectors.joining(", "))
                            );
                        }
                        this.escapeLaunchPad(
                                launchPad.vertical,
                                supersetEntries.stream().map(Map.Entry::getKey).collect(Collectors.toList()),
                                localVisitees
                        );
                        this.launchPadIndexLock.unlock();
                        continue;
                    }
                } catch (InterruptedException e) {
                    // Pass.
                }
                synchronized (this.launchPads) {
                    this.launchPads.add(launchPad);
                }
                System.out.printf("Cowardly giving up on %s.\n", this.strategy.format(launchPad.vertical));
                return null;
            }
        }


        /**
         * Whenever a launch pad is superset-pruned, we cannot simply discard it. Instead, we need to move it out of the
         * "pruning shadow". Specifically, we need to supplement the launch pad with minimum hitting sets
         * of the inversed {@code pruningSupersets}, because these ensure that we add a minimum amount of {@link Column}s
         * to the launch pad, such that these new candidates are not a subset of any pruning superset any more.
         * In addition, we might need to adhere to {@code scope} within which the escaped launch pads must lie.
         *
         * @param launchPad        that is pruned
         * @param pruningSupersets that prune
         * @param localVisitees    known (non-)dependencies
         */
        private void escapeLaunchPad(Vertical launchPad,
                                     List<Vertical> pruningSupersets,
                                     VerticalMap<VerticalInfo> localVisitees) {

            // Invert the pruning supersets.
            List<Vertical> invertedPruningSupersets = pruningSupersets.stream()
                    .map(Vertical::invert)
                    .map(v -> v.without(strategy.getIrrelantColumns()))
                    .collect(Collectors.toList());

            // Calculate the hitting set.
            Collection<Vertical> hittingSet = Pyro.this.relation.calculateHittingSet(
                    invertedPruningSupersets,
                    hittingSetCandidate -> {
                        // Check if the candidate is within the scope.
                        if (this.scope != null && this.scope.getSupersetEntries(hittingSetCandidate).isEmpty()) {
                            return true;
                        }

                        Vertical launchPadCandidate = launchPad.union(hittingSetCandidate);

                        // Check if the candidate is pruned.
                        if ((localVisitees != null && isImpliedByMinDep(launchPadCandidate, localVisitees))
                                || isImpliedByMinDep(launchPadCandidate, globalVisitees)) {
                            return true;
                        }

                        // Check if the candidate is covered by an existing seed.
                        if (!this.launchPadIndex.getSubsetEntries(launchPadCandidate).isEmpty()) {
                            return true;
                        }

                        return false;
                    }
            );

            synchronized (this.launchPads) {
                // Create and store the launchPads.
                for (Vertical escaping : hittingSet) {
                    Vertical escapedLaunchPadVertical = launchPad.union(escaping);
                    DependencyCandidate escapedLaunchPad = strategy.createDependencyCandidate(escapedLaunchPadVertical);
                    debug("  Escaped: %s\n", strategy.format(escapedLaunchPadVertical));
                    this.launchPads.add(escapedLaunchPad);
                    this.launchPadIndex.put(escapedLaunchPad.vertical, escapedLaunchPad);
                }
            }
        }

        /**
         * Return a launch pad to the {@link #launchPads} or {@link #deferredLaunchPads}.
         *
         * @param launchPad to be added
         */
        void addLaunchPad(DependencyCandidate launchPad) {
            synchronized (this.launchPads) {
                this.launchPads.add(launchPad);
                this.launchPadIndex.put(launchPad.vertical, launchPad);
            }
        }

        /**
         * Return a launch pad to the {@link #launchPads} or {@link #deferredLaunchPads}.
         *
         * @param isDefer whether the {@code launchPad} <b>can</b> be deferred
         */
        void returnLaunchPad(DependencyCandidate launchPad, boolean isDefer) {
            synchronized (this.launchPads) {
                if (isDefer && Pyro.this.isDeferFailedLaunchPads) {
                    this.deferredLaunchPads.add(launchPad);
                    debug("Deferred seed.\n", this.strategy.format(launchPad.vertical));
                } else {
                    this.launchPads.add(launchPad);
                }
                this.launchPadIndex.put(launchPad.vertical, launchPad);
            }
        }

        /**
         * Take a {@code launchPad} and extend it until we meet a dependency (or hit a maximum non-dependency). The start to
         * {@link #trickleDown(Vertical, double, VerticalMap)}.
         *
         * @param launchPad     from which to ascend
         * @param localVisitees stores local dependencies and non-dependencies
         * @return whether a dependency was met
         */
        private boolean ascend(DependencyCandidate launchPad, VerticalMap<VerticalInfo> localVisitees) {
            long _startMillis = System.currentTimeMillis();

            info("===== Ascending from from %s ======\n", this.strategy.format(launchPad.vertical));

            // Check whether we should resample.
            if (this.strategy.shouldResample(launchPad.vertical, this.sampleBoost)) {
                Pyro.this.createFocusedSample(launchPad.vertical, sampleBoost);
            }

            // Start the ascension towards a dependency or a (local) maximum non-dependency.
            DependencyCandidate traversalCandidate = launchPad;
            double error;
            while (true) {
                debug("-> %s\n", strategy.format(traversalCandidate.vertical));

                if (Pyro.this.isCheckEstimates) {
                    this.checkEstimate(strategy, traversalCandidate);
                }

                // Primarily, check if we should operate on agree sets or PLIs.
                if (traversalCandidate.isExact()) {
                    // If we have an exhaustive sample, we should operate upon the agree sets.
                    error = traversalCandidate.error.get();

                    // Store the candidate's state.
                    localVisitees.put(traversalCandidate.vertical, new VerticalInfo(
                            error <= strategy.maxError,
                            false,
                            error
                    ));

                    // Check whether to stop the ascension.
                    if (error <= strategy.maxError) break;

                } else { // We have no exact sample.
                    // Are we confident that this is _not_ a dependency?
                    if (traversalCandidate.error.getMin() > strategy.maxError) {
                        // If we are confident that we do not have a key, than we skip the check.
                        debug("   Skipping check for %s (estimated error: %s).\n",
                                strategy.format(traversalCandidate.vertical),
                                traversalCandidate.error
                        );
                        error = Double.NaN;

                    } else {
                        // If we are not confident that we do not have a dependency, then we better check our candidate.
                        error = strategy.calculateError(traversalCandidate.vertical);

                        // Store the candidate's state.
                        localVisitees.put(traversalCandidate.vertical, new VerticalInfo(
                                error <= strategy.maxError,
                                false,
                                error
                        ));

                        // Check whether to stop the ascension.
                        if (error <= strategy.maxError) break;

                        // Potentially create a new agree set sample.
                        if (strategy.shouldResample(traversalCandidate.vertical, sampleBoost)) {
                            Pyro.this.createFocusedSample(traversalCandidate.vertical, sampleBoost);
                        }

                    }
                }

                // If we did not find a key, we try to discover further up.
                // Check if we can ascend in the first place.
                if (traversalCandidate.vertical.getArity() >= Pyro.this.relation.getNumColumns() - this.strategy.getNumIrrelevantColumns())
                    break;

                // Select the most promising direct superset vertical.
                DependencyCandidate nextCandidate = null;
                // Or select a random candidate: We implement this with a size-one reservoir sampling.
                int numSeenElements = this.isAscendRandomly ? 1 : -1;
                for (Column extensionColumn : Pyro.this.relation.getColumns()) {
                    if (traversalCandidate.vertical.getColumnIndices().get(extensionColumn.getIndex())
                            || strategy.isIrrelevantColumn(extensionColumn)) {
                        continue;
                    }
                    Vertical extendedVertical = traversalCandidate.vertical.union(extensionColumn);

                    // Is the extension allowed?
                    if (scope != null && scope.getSupersetEntries(extendedVertical).isEmpty()) {
                        continue;
                    }

                    // Make sure that the extended key is not subset-pruned.
                    boolean isSubsetPruned = isImpliedByMinDep(extendedVertical, globalVisitees);
                    if (isSubsetPruned) {
                        continue;
                    }
                    // Obtain the error for the new candidate.
                    DependencyCandidate extendedCandidate = strategy.createDependencyCandidate(extendedVertical);
                    // Update the best candidate.
                    if (nextCandidate == null
                            || numSeenElements == -1 && extendedCandidate.error.getMean() < nextCandidate.error.getMean()
                            || numSeenElements != -1 && Pyro.this.random.nextInt(++numSeenElements) == 0) {
                        nextCandidate = extendedCandidate;
                    }
                }

                // If we found a candidate, then follow that one now.
                if (nextCandidate != null) {
                    traversalCandidate = nextCandidate;
                } else {
                    break;
                }
            } // Climbing

            // Beware of situations, where we went all the way up without having verified the dependency error at all.
            if (Double.isNaN(error)) {
                // Evaluate the key error.
                debug("   Hit the ceiling at %s.\n", strategy.format(traversalCandidate.vertical));
                error = strategy.calculateError(traversalCandidate.vertical);
                debug("   Checking candidate... actual error: %.5f\n", error);
            }

            _profilingData.ascendMillis.addAndGet(System.currentTimeMillis() - _startMillis);
            _profilingData.numAscends.incrementAndGet();

            if (error <= strategy.maxError) {
                // If we reached a desired key, then we need to minimize it now.
                debug("   Key peak in climbing phase: e(%s)=%,.3f -> Need to minimize.\n", traversalCandidate.vertical, error);
                this.trickleDown(traversalCandidate.vertical, error, localVisitees);

                if (recursionDepth == 0) { // Top-level ascension?
                    assert scope == null;
                    // Add a minimal dependency token to prune both supersets and subsets.
                    globalVisitees.put(traversalCandidate.vertical, VerticalInfo.forMinimalDependency());
                }

                return true;

            } else {
                if (recursionDepth == 0) { // Top-level ascension?
                    assert scope == null;
                    globalVisitees.put(traversalCandidate.vertical, VerticalInfo.forMaximalNonDependency());
                    info("[---] %s is maximum non-dependency (err=%,.3f).\n", traversalCandidate.vertical, error);
                } else {
                    localVisitees.put(traversalCandidate.vertical, VerticalInfo.forNonDependency());
                    info("      %s is local-maximum non-dependency (err=%,.3f).\n", traversalCandidate.vertical, error);
                }

                return false;
            }
        }

        /**
         * Debug method to verify whether error estimates are correct. In particular, exact estimates must be correct.
         */
        private void checkEstimate(DependencyStrategy strategy, DependencyCandidate traversalCandidate) {
            double actualError = strategy.calculateError(traversalCandidate.vertical);
            double diff = actualError - traversalCandidate.error.getMean();
            boolean isEstimateCorrect = traversalCandidate.error.getMin() <= actualError && actualError <= traversalCandidate.error.getMax();
            info("Estimate check for %s. Status: %s, estimate: %s, actual: %,.03f, delta: %+,.03f\n",
                    strategy.format(traversalCandidate.vertical), isEstimateCorrect ? "correct" : "erroneous", traversalCandidate.error, actualError, diff);

            {
                strategy.calculateError(traversalCandidate.vertical);
                strategy.createDependencyCandidate(traversalCandidate.vertical);
            }

            if (!isEstimateCorrect && traversalCandidate.isExact()) {
                throw new IllegalStateException("There seems to be a problem with the error calculation.");
            }
        }

        /**
         * Finds and registers minimal {@link PartialKey}s below the given {@code keyCandidate}.
         *
         * @param mainPeak      potentially a partial key whose minimal partial keys are sought
         * @param mainPeakError the error of the {@code keyCandidate} or {@link Double#NaN} if it is not known
         * @param localVisitees known non-keys in the current search round
         */
        private void trickleDown(Vertical mainPeak, double mainPeakError, VerticalMap<VerticalInfo> localVisitees) {
            long _startMillis = System.currentTimeMillis();

            info("===== Trickling down from %s ======\n", strategy.format(mainPeak));

            // Prepare to collect the (alleged) minimal and maximal (non-)dependencies.
            Set<Vertical> maximalNonDeps = new HashSet<>();
            VerticalMap<VerticalInfo> allegedMinDeps = new VerticalMap<>(Pyro.this.relation);

            // TODO: This does not make sense, does it?
//        // Bootstrap the alleged minimum dependencies with known minimum dependencies.
//        for (Map.Entry<Vertical,VerticalInfo> entry : globalVisitees.getSubsetEntries(mainPeak)) {
//            if (entry.getValue().isDependency && entry.getValue().isExtremal) {
//                this.info("* Putting known minimum dependency %s.\n", strategy.format(entry.getKey()));
//                allegedMinDeps.put(entry.getKey(), entry.getValue());
//            }
//        }
//        for (Map.Entry<Vertical,VerticalInfo> entry : localVisitees.getSubsetEntries(mainPeak)) {
//            if (entry.getValue().isDependency && entry.getValue().isExtremal) {
//                allegedMinDeps.put(entry.getKey(), entry.getValue());
//                this.info("* Putting known minimum dependency %s.\n", strategy.format(entry.getKey()));
//            }
//        }

            // The peaks are the points from which we need to trickle down. We start trickling down from the lowest peaks
            // hoping that they are closer to the minimum dependencies.
            // TODO: See whether this really pays off.
            PriorityQueue<DependencyCandidate> peaks = new PriorityQueue<>(DependencyCandidate.arityComparator);
            peaks.add(new DependencyCandidate(mainPeak, new ConfidenceInterval(mainPeakError, mainPeakError), null));

            // Keep track of visited nodes, so as to visit no node twice.
            Set<Vertical> allegedNonDeps = new HashSet<>();

            // Trickle down from all peaks that we have.
            long _lastUpdateMillis = System.currentTimeMillis();
            while (!peaks.isEmpty()) {
                DependencyCandidate peak = peaks.peek();

                if (System.currentTimeMillis() - _lastUpdateMillis > 1000L) {
                    info("--- %,d peaks (%s)\n", peaks.size(), formatArityHistogram(peaks.stream().map(dc -> dc.vertical).collect(Collectors.toList())));
                    info("    %,d alleged minimum deps (%s)\n", allegedMinDeps.size(), formatArityHistogram(allegedMinDeps));
                    _lastUpdateMillis = System.currentTimeMillis();
                }

                // At the very first instance, we need to check whether the peak is not pruned.
                // If the peak is pruned by an (alleged) dependency, then this dependency must be within our current
                // sub-lattice.
                // If there are such dependencies, we need to escape the peak.
                Collection<Vertical> subsetDeps = getSubsetDeps(peak.vertical, allegedMinDeps);
                if (!subsetDeps.isEmpty()) {
                    // Remove the old peak.
                    peaks.poll();

                    // Do the escaping.
                    Collection<Vertical> escapedPeakVerticals =
                            Pyro.this.relation.calculateHittingSet(subsetDeps, null).stream()
                                    .map(peak.vertical::without)
                                    .collect(Collectors.toList());

                    // For escaped peaks, we want to make sure that we deem them to be dependencies.
                    // Otherwise, we may discard them.
                    for (Vertical escapedPeakVertical : escapedPeakVerticals) {
                        if (escapedPeakVertical.getArity() > 0 && !allegedNonDeps.contains(escapedPeakVertical)) {
                            DependencyCandidate escapedPeak = strategy.createDependencyCandidate(escapedPeakVertical);

                            if (escapedPeak.error.getMean() > strategy.maxError) {
                                allegedNonDeps.add(escapedPeak.vertical);
                                continue;
                            }
                            if (isKnownNonDependency(escapedPeakVertical, localVisitees)
                                    || isKnownNonDependency(escapedPeakVertical, globalVisitees)) {
                                continue;
                            }

                            peaks.add(escapedPeak);
                        }
                    }
                    continue;
                }

                // Find an alleged minimum dependency for the peak.
                Vertical allegedMinDep = this.trickleDownFrom(peak, strategy, allegedMinDeps, allegedNonDeps, localVisitees, globalVisitees, sampleBoost);
                if (allegedMinDep == null) {
                    // If we could not find an alleged minimum dependency, that means that we do not believe that the
                    // peak itself is a dependency. Hence, we remove it.
                    peaks.poll();
                }
            } // hypothesize minimum dependencies
            info("* %,d alleged minimum dependencies (%s)\n", allegedMinDeps.size(), formatArityHistogram(allegedMinDeps));

            // Register already-known-to-be-minimal dependencies.
            int numUncertainMinDeps = 0;
            for (Map.Entry<Vertical, VerticalInfo> entry : allegedMinDeps.entrySet()) {
                Vertical allegedMinDep = entry.getKey();
                VerticalInfo info = entry.getValue();
                if (info.isExtremal && !globalVisitees.containsKey(allegedMinDep)) {
                    info("[%3d] Minimum dependency: %s (error=%,.03f)\n",
                            recursionDepth, strategy.format(allegedMinDep), info.error
                    );
                    globalVisitees.put(allegedMinDep, info);
                    strategy.registerDependency(allegedMinDep, info.error);
                }
                if (!info.isExtremal) numUncertainMinDeps++;
            }

            // NB: We must NOT stop if all dependencies are known to be minimal because they might not be complete!

            info("* %,d/%,d alleged minimum dependencies might be non-minimal.\n",
                    numUncertainMinDeps, allegedMinDeps.size()
            );

            // We have an initial hypothesis about where the minimal dependencies are.
            // Now, we determine the corresponding maximal non-dependencies.
            List<Vertical> allegedMaxNonDeps = Pyro.this.relation.calculateHittingSet(allegedMinDeps.keySet(), null).stream()
                    .map(minLeaveOutVertical -> minLeaveOutVertical.invert(mainPeak))
                    .collect(Collectors.toList());
            info("* %,d alleged maximum non-dependencies (%s)\n", allegedMaxNonDeps.size(), formatArityHistogram(allegedMaxNonDeps));

            // Here, we check the consistency of all data structures.
            assert allegedMinDeps.keySet().stream().allMatch(mainPeak::contains) : String.format("Illegal min deps: %s.", allegedMinDeps);
            assert allegedMaxNonDeps.stream().allMatch(mainPeak::contains) : String.format("Illegal max non-deps: %s.", allegedMaxNonDeps);

            // Validate the alleged maximal dependencies.
            for (Vertical allegedMaxNonDep : allegedMaxNonDeps) {
                // Is it the empty vertical?
                if (allegedMaxNonDep.getArity() == 0) continue;
                // Is it a known non-dep?
                if (maximalNonDeps.contains(allegedMaxNonDep)
                        || isKnownNonDependency(allegedMaxNonDep, localVisitees)
                        || isKnownNonDependency(allegedMaxNonDep, globalVisitees)) {
                    continue;
                }
                // We don't check whether it's a known dependency, because if so, it could not be an alleged maximal non-dependency.

                // Check and evaluate the candidate.
                double error = strategy.calculateError(allegedMaxNonDep);
                boolean isNonDep = error > strategy.maxError;
                debug("* Alleged maximal non-dependency %s: non-dep?: %b, error: %.03f\n",
                        strategy.format(allegedMaxNonDep), isNonDep, error);
                if (isNonDep) {
                    // If the candidate is a non-dependency, it must be maximal (below the peak).
                    maximalNonDeps.add(allegedMaxNonDep);
                    localVisitees.put(allegedMaxNonDep, VerticalInfo.forNonDependency());

                } else {
                    // If the candidate is not a non-dependency, then there must be a dependency below that is not in
                    // entailed by our alleged minimal dependencies. Hence, the candidate becomes a new peak.
                    // Note in particular, that all missed minimal dependencies must be covered by any such false
                    // maximum non-dependency candidate.
                    peaks.add(new DependencyCandidate(allegedMaxNonDep, new ConfidenceInterval(error, error), null));
                }
            }

            if (peaks.isEmpty()) {
                // When all maximal non-dependency candidates are correct (and, thus, we have no more peaks), we are done.
                // We only need to register the previously uncertain minimum dependencies.
                for (Map.Entry<Vertical, VerticalInfo> entry : allegedMinDeps.entrySet()) {
                    Vertical allegedMinDep = entry.getKey();
                    VerticalInfo info = entry.getValue();
                    if (!info.isExtremal && !globalVisitees.containsKey(allegedMinDep)) {
                        info("[%3d] Minimum dependency: %s (error=%,.03f)\n",
                                recursionDepth, strategy.format(allegedMinDep), info.error
                        );
                        info.isExtremal = true;
                        globalVisitees.put(allegedMinDep, info);
                        strategy.registerDependency(allegedMinDep, info.error);
                    }
                }

            } else {
                // Otherwise, we have to continue our search.
                // For that matter, we restrict the search space and re-start the discovery there.
                info("* %,d new peaks (%s).\n", peaks.size(), formatArityHistogram(peaks.stream().map(dc -> dc.vertical).collect(Collectors.toList())));

                // Define the upper bound for the following dependency search: all the alleged minimal dependencies.
                VerticalMap<Vertical> newScope = new VerticalMap<>(Pyro.this.relation);
                for (DependencyCandidate peak : peaks) {
                    newScope.put(peak.vertical, peak.vertical);
                }

                // We did not do a good enough job regarding the estimation. Therefore, increase the sampling size.
                double newSampleBoost = sampleBoost * this.sampleBoost;
                info("* Increasing sampling boost factor to %,.1f.\n", newSampleBoost);


                SearchSpace nestedSearchSpace = new SearchSpace(this.strategy,
                        newScope,
                        this.globalVisitees,
                        this.recursionDepth + 1,
                        this.sampleBoost * Pyro.this.sampleBooster
                );
                // Define the lower bound for the following dependency search.
                Set<Column> scopeColumns = newScope.keySet().stream()
                        .flatMap(vertical -> Arrays.stream(vertical.getColumns()))
                        .collect(Collectors.toSet());
                for (Column scopeColumn : scopeColumns) {
                    nestedSearchSpace.addLaunchPad(strategy.createDependencyCandidate(scopeColumn));
                }
                long _breakStartMillis = System.currentTimeMillis();
                nestedSearchSpace.discover(localVisitees);
                _startMillis -= System.currentTimeMillis() - _breakStartMillis;

                // Finally, we need to check whether some of our alleged minimal dependencies are actually minimal.
                // We have to check them, because they lie outside of the scope and, thus, cannot be discovered in the
                // above recursion.
                for (Map.Entry<Vertical, VerticalInfo> entry : allegedMinDeps.entrySet()) {
                    Vertical allegedMinDep = entry.getKey();
                    VerticalInfo info = entry.getValue();
                    if (!isImpliedByMinDep(allegedMinDep, globalVisitees)) {
                        info("[%3d] Minimum dependency: %s (error=%,.03f) (was right after all)\n",
                                recursionDepth, strategy.format(allegedMinDep), info.error
                        );
                        info.isExtremal = true;
                        globalVisitees.put(allegedMinDep, info);
                        strategy.registerDependency(allegedMinDep, info.error);
                    }
                }
            }

            _profilingData.trickleDownMillis.addAndGet(System.currentTimeMillis() - _startMillis);
            _profilingData.numTrickleDowns.incrementAndGet();
        }

        /**
         * Recursively check this {@link DependencyCandidate} and its parents to find a dependency that is deemed to
         * be minimal.
         *
         * @param minDepCandidate a {@link DependencyCandidate} that should be deemed to be a candidate
         * @return some alleged minimum dependency or {@code null} if it is believed that there is no such dependency
         */

        private Vertical trickleDownFrom(DependencyCandidate minDepCandidate,
                                         DependencyStrategy strategy,
                                         VerticalMap<VerticalInfo> allegedMinDeps,
                                         Set<Vertical> allegedNonDeps,
                                         VerticalMap<VerticalInfo> localVisitees,
                                         VerticalMap<VerticalInfo> globalVisitees,
                                         double boostFactor) {
            assert minDepCandidate.error.getMin() <= strategy.maxError;

            // Enumerate the parents of our candidate to check if any of them might be a dependency.
            boolean areAllParentsKnownNonDeps = true;
            if (minDepCandidate.vertical.getArity() > 1) {
                // Create candidates for the parent verticals.
                PriorityQueue<DependencyCandidate> parentCandidates = new PriorityQueue<>(
                        DependencyCandidate.minErrorComparator
                );
                for (Vertical parentVertical : minDepCandidate.vertical.getParents()) {
                    // Check if the parent vertical is a known non-dependency.
                    if (isKnownNonDependency(parentVertical, localVisitees)
                            || isKnownNonDependency(parentVertical, globalVisitees)) continue;
                    // Avoid double visits.
                    if (allegedNonDeps.contains(parentVertical)) {
                        areAllParentsKnownNonDeps = false;
                        continue;
                    }
                    // Estimate the error.
                    parentCandidates.add(strategy.createDependencyCandidate(parentVertical));
                }

                // Check the parent candidates with relevant errors.
                while (!parentCandidates.isEmpty()) {
                    DependencyCandidate parentCandidate = parentCandidates.poll();
                    // We can stop as soon as the unchecked parent candidate with the least error is not deemed to be a dependency.
                    // Additionally, we distinguish whether the parents are known non-dependencies or we just deem them so.
                    if (parentCandidate.error.getMin() > strategy.maxError) {
                        // Additionally, we mark all remaining candidates as alleged non-dependencies to avoid revisits.
                        do {
                            if (parentCandidate.isExact()) {
                                localVisitees.put(parentCandidate.vertical, VerticalInfo.forNonDependency());
                            } else {
                                allegedNonDeps.add(parentCandidate.vertical);
                                areAllParentsKnownNonDeps = false;
                            }
                            parentCandidate = parentCandidates.poll();
                        } while (parentCandidate != null);
                        break;
                    }

                    // Otherwise, we will have to recursively check the candidate.
                    Vertical allegedMinDep = this.trickleDownFrom(
                            parentCandidate,
                            strategy,
                            allegedMinDeps,
                            allegedNonDeps,
                            localVisitees,
                            globalVisitees,
                            boostFactor
                    );
                    // Stop immediately, when an alleged minimum dependency has been found.
                    if (allegedMinDep != null) return allegedMinDep;

                    // Otherwise, we try to update our minimum dependency candidate and see if we still deem the current
                    // candidate to be a dependency.
                    if (!minDepCandidate.isExact()) {
                        // In particular, we test if this very node is a dependency itself. This is supposedly not expensive
                        // because we just falsified a parent.
                        double error = strategy.calculateError(minDepCandidate.vertical);
                        minDepCandidate = new DependencyCandidate(minDepCandidate.vertical, new ConfidenceInterval(error, error), null);
                        if (error > strategy.maxError) break;
                    }
                }
            }

            // At this point, we deem none of the parents to be dependencies.
            // Still, we need to check if our candidate is an actual dependency.
            double candidateError = minDepCandidate.isExact() ?
                    minDepCandidate.error.get() :
                    strategy.calculateError(minDepCandidate.vertical);
            if (candidateError <= strategy.maxError) {
                // TODO: I think, we don't need to add the dep to the localVisitees, because we won't visit it anymore.
                debug("* Found %d-ary minimum dependency candidate: %s\n", minDepCandidate.vertical.getArity(), minDepCandidate);
                allegedMinDeps.removeSupersetEntries(minDepCandidate.vertical);
                allegedMinDeps.put(minDepCandidate.vertical, new VerticalInfo(true, areAllParentsKnownNonDeps, candidateError));
                if (areAllParentsKnownNonDeps && Pyro.this.isCheckEstimates) {
                    requireMinimalDependency(strategy, minDepCandidate.vertical);
                }
                return minDepCandidate.vertical;

            } else {
                debug("* Guessed incorrect %d-ary minimum dependency candidate.\n", minDepCandidate.vertical.getArity());
                localVisitees.put(minDepCandidate.vertical, new VerticalInfo(false, false));

                // If we had a wrong guess, we re-sample so as to provide insights for the child vertices.
                if (strategy.shouldResample(minDepCandidate.vertical, boostFactor)) {
                    Pyro.this.createFocusedSample(minDepCandidate.vertical, boostFactor);
                }
                return null;
            }
        }

        @Override
        public String toString() {
            return String.format("%s[%s]", this.getClass().getSimpleName(), this.strategy);
        }
    }


    public static class ProfilingData {
        public final AtomicLong probingNanos = new AtomicLong(0L);
        public final AtomicLong numProbings = new AtomicLong(0L);
        public final AtomicLong ascendMillis = new AtomicLong(0L);
        public final AtomicLong numAscends = new AtomicLong(0L);
        public final AtomicLong trickleDownMillis = new AtomicLong(0L);
        public final AtomicLong numTrickleDowns = new AtomicLong(0L);
    }

    /**
     * Encapsulates logic to create {@link DependencyCandidate}s and verify them.
     */
    private abstract class DependencyStrategy {

        final double maxError;

        protected DependencyStrategy(double maxError) {
            this.maxError = maxError;
        }

        abstract DependencyCandidate createDependencyCandidate(Vertical candidate);

        abstract double calculateError(Vertical candidate);

        abstract String format(Vertical vertical);

        abstract void registerDependency(Vertical vertical, double error);

        abstract boolean isIrrelevantColumn(int columnIndex);

        boolean isIrrelevantColumn(Column column) {
            return this.isIrrelevantColumn(column.getIndex());
        }

        abstract int getNumIrrelevantColumns();

        /**
         * Decide whether it is worthwhile to create a focused sampling on the given {@link Vertical}.
         *
         * @param vertical
         * @return
         */
        boolean shouldResample(Vertical vertical, double boostFactor) {
            if (Pyro.this.sampleSize <= 0) return false;

            // Do we have an exact sample already?
            AgreeSetSample currentSample = Pyro.this.getAgreeSetSample(vertical);
            if (currentSample.isExact()) return false;

            // Get an estimate of the number of equality pairs in the vertical.
            PositionListIndex pli = vertical.tryGetPositionListIndex();
            double nep = pli != null ?
                    pli.getNep() :
                    currentSample.estimateAgreements(vertical) * Pyro.this.relation.getNumTuplePairs();

            // Would the new sample be exact?
            if (nep <= Pyro.this.sampleSize * boostFactor) return true;

            // Will we achieve an improved sampling ratio?
            double newSamplingRatio = Pyro.this.sampleSize * boostFactor / nep;
            return newSamplingRatio >= 2 * currentSample.getSamplingRatio();


            // Only if we have the PLI.
//            PositionListIndex pli = vertical.tryGetPositionListIndex();
//            if (pli == null) return false;
//
//            // Do we have an exact sample already?
//            AgreeSetSample currentSample = Pyro.this.getAgreeSetSample(vertical);
//            if (currentSample.isExact()) return false;
//
//            // Would the new sample be exact?
//            if (pli.getNep() <= Pyro.this.sampleSize * boostFactor) return true;
//
//            // Will we achieve an improved sampling ratio?
//            double newSamplingRatio = Pyro.this.sampleSize * boostFactor / pli.getNep();
//            return newSamplingRatio >= 2 * currentSample.getSamplingRatio();
        }

        public abstract Vertical getIrrelantColumns();
    }

    /**
     * {@link DependencyStrategy} implementation for partial keys.
     */
    private class KeyG1Strategy extends DependencyStrategy {

        private KeyG1Strategy(double maxError) {
            super(maxError);
        }

        @Override
        double calculateError(Vertical keyCandidate) {
            PositionListIndex pli = keyCandidate.getPositionListIndex(Pyro.this.pliCache);
            return this.calculateKeyError(pli);
        }

        private double calculateKeyError(PositionListIndex pli) {
            return this.calculateKeyError(pli.getNep());
        }

        private double calculateKeyError(double numEqualityPairs) {
            double keyError = numEqualityPairs / Pyro.this.relation.getNumTuplePairs();
            // We truncate some precision here to avoid small numerical flaws to affect the result.
            return PFDRater.round(keyError);
        }

        private ConfidenceInterval calculateKeyError(ConfidenceInterval estimatedEqualityPairs) {
            return new ConfidenceInterval(
                    this.calculateKeyError(estimatedEqualityPairs.getMin()),
                    this.calculateKeyError(estimatedEqualityPairs.getMax())
            );
        }

        @Override
        DependencyCandidate createDependencyCandidate(Vertical vertical) {
            assert Pyro.this.keyErrorRater == PFDRater.nepPerTupleKeyErrorRater;

            // If we have columns, there is no need to estimate.
            if (vertical.getArity() == 1) {
                double keyError = this.calculateKeyError((long) vertical.getNep());
                return new DependencyCandidate(vertical, new ConfidenceInterval(keyError, keyError), null);
            }

            if (Pyro.this.agreeSetSamples == null) {
                return new DependencyCandidate(vertical, new ConfidenceInterval(0, 1), null);
            }

            // Find the best available correlation provider.
            AgreeSetSample agreeSetSample = Pyro.this.getAgreeSetSample(vertical);
            ConfidenceInterval estimatedEqualityPairs = agreeSetSample
                    .estimateAgreements(vertical, Pyro.this.estimateConfidence)
                    .multiply(Pyro.this.relation.getNumTuplePairs());
            ConfidenceInterval keyError = this.calculateKeyError(estimatedEqualityPairs);
            return new DependencyCandidate(vertical, keyError, agreeSetSample);
        }

        @Override
        String format(Vertical vertical) {
            return String.format("key(%s)", vertical);
        }

        @Override
        void registerDependency(Vertical vertical, double error) {
            // TODO: Calculate score.
            Pyro.this.registerKey(vertical, error, Double.NaN);
        }

        @Override
        boolean isIrrelevantColumn(int columnIndex) {
            return false;
        }

        @Override
        int getNumIrrelevantColumns() {
            return 0;
        }

        @Override
        public Vertical getIrrelantColumns() {
            return Pyro.this.relation.emptyVertical;
        }

        @Override
        public String toString() {
            return String.format("Key[g1\u2264%.3f]", this.maxError);
        }
    }

    private static String formatArityHistogram(VerticalMap<?> verticalMap) {
        return formatArityHistogram(verticalMap.keySet());
    }

    private static String formatArityHistogram(Collection<Vertical> verticals) {
        Int2IntSortedMap arityCounter = new Int2IntAVLTreeMap();
        arityCounter.defaultReturnValue(0);
        verticals.stream()
                .mapToInt(Vertical::getArity)
                .forEach(arity -> arityCounter.merge(arity, 1, (oldValue, one) -> oldValue + one));
        return arityCounter.int2IntEntrySet().stream()
                .map(entry -> String.format("%,dx %d-ary", entry.getIntValue(), entry.getIntKey()))
                .collect(Collectors.joining(", "));
    }


    /**
     * {@link DependencyStrategy} implementation for partial keys.
     */
    private class FdG1Strategy extends DependencyStrategy {

        private final Column rhs;

        private FdG1Strategy(Column rhs, double maxError) {
            super(maxError);
            this.rhs = rhs;
        }

        @Override
        double calculateError(Vertical fdCandidate) {
            // Special case: Check 0-ary FD.
            if (fdCandidate.getArity() == 0) {
                return this.calculateG1(this.rhs.getNip());
            }

            PositionListIndex pli = fdCandidate.getPositionListIndex(Pyro.this.pliCache);
            return this.calculateG1(pli);
        }

        private double calculateG1(PositionListIndex lhsPli) {
            long pliNanos = System.nanoTime();
            long numViolations = 0L;
            final Int2IntOpenHashMap valueCounts = new Int2IntOpenHashMap();
            valueCounts.defaultReturnValue(0);
            final int[] probingTable = this.rhs.getData();

            // Do the actual probing cluster by cluster.
            for (IntArrayList cluster : lhsPli.getIndex()) {
                valueCounts.clear();
                for (IntIterator iterator = cluster.iterator(); iterator.hasNext(); ) {

                    // Probe the position.
                    final int position = iterator.nextInt();
                    final int probingTableValueId = probingTable[position];

                    // Count the probed position if it is not a singleton.
                    if (probingTableValueId != singletonValueId) {
                        valueCounts.addTo(probingTableValueId, 1);
                    }
                }

                // Count the violations within the cluster.
                long numViolationsInCluster = cluster.size() * (cluster.size() - 1L) >> 1;
                ObjectIterator<Int2IntMap.Entry> valueCountIterator = valueCounts.int2IntEntrySet().fastIterator();
                while (valueCountIterator.hasNext()) {
                    int refinedClusterSize = valueCountIterator.next().getIntValue();
                    numViolationsInCluster -= refinedClusterSize * (refinedClusterSize - 1L) >> 1;
                }
                numViolations += numViolationsInCluster;
            }
            pliNanos = System.nanoTime() - pliNanos;
            Pyro.this._profilingData.probingNanos.addAndGet(pliNanos);
            Pyro.this._profilingData.numProbings.incrementAndGet();

            return this.calculateG1(numViolations);
        }

        private double calculateG1(double numViolatingTuplePairs) {
            double g1 = numViolatingTuplePairs / Pyro.this.relation.getNumTuplePairs();
            // We truncate some precision here to avoid small numerical flaws to affect the result.
            return PFDRater.round(g1);
        }

        private ConfidenceInterval calculateG1(ConfidenceInterval numViolations) {
            return new ConfidenceInterval(
                    this.calculateG1(numViolations.getMin()),
                    this.calculateG1(numViolations.getMax())
            );
        }

        @Override
        DependencyCandidate createDependencyCandidate(Vertical vertical) {
            assert Pyro.this.fdErrorRater == PFDRater.g1prime;

            if (Pyro.this.agreeSetSamples == null) {
                return new DependencyCandidate(vertical, new ConfidenceInterval(0, 1), null);
            }

            // Find the best available correlation provider.
            AgreeSetSample agreeSetSample = Pyro.this.getAgreeSetSample(vertical);
            ConfidenceInterval numViolatingTuplePairs = agreeSetSample
                    .estimateMixed(vertical, this.rhs, Pyro.this.estimateConfidence)
                    .multiply(Pyro.this.relation.getNumTuplePairs());

            ConfidenceInterval g1 = this.calculateG1(numViolatingTuplePairs);
            return new DependencyCandidate(vertical, g1, agreeSetSample);
        }

        @Override
        String format(Vertical vertical) {
            return String.format("%s\u2192%s", vertical, this.rhs);
        }

        @Override
        void registerDependency(Vertical vertical, double error) {
            // TODO: Calculate score.
            Pyro.this.registerFd(vertical, this.rhs, vertical.union(this.rhs), error, Double.NaN, false);
        }

        @Override
        boolean isIrrelevantColumn(int columnIndex) {
            return this.rhs.getIndex() == columnIndex;
        }

        @Override
        int getNumIrrelevantColumns() {
            return 1;
        }

        @Override
        public Vertical getIrrelantColumns() {
            return this.rhs;
        }

        @Override
        public String toString() {
            return String.format("FD[RHS=%s, g1\u2264%.3f]", this.rhs.getName(), this.maxError);
        }
    }

    private final static class TrickleStats {

        int numChecks = 0, numPliChecks = 0;

        int numDepMispredictions = 0, numNonDepMispredictions = 0;

    }

}
