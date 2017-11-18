package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.PartialKey;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.ConfidenceInterval;
import de.hpi.isg.pyro.util.SynchronizedVerticalMap;
import de.hpi.isg.pyro.util.VerticalMap;
import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2IntSortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * This class describes a search space from that dependencies can be discovered. In particular, it also defines
 * Pyro's traversal logic to do this very discovery.
 */
public class SearchSpace implements Serializable {

    private static Logger logger = LoggerFactory.getLogger(SearchSpace.class);

    private transient ProfilingContext context;

    /**
     * Used to identify this instance.
     */
    public final int id;

    /**
     * Interrupt flag to tell workers to stop processing this instance (but leave it in a consistent state).
     */
    private boolean interruptFlag = false;

    final DependencyStrategy strategy;

    final VerticalMap<VerticalInfo> globalVisitees;

    final SortedSet<DependencyCandidate> launchPads;

    final VerticalMap<DependencyCandidate> launchPadIndex;

    final Lock launchPadIndexLock = new ReentrantLock();

    final LinkedList<DependencyCandidate> deferredLaunchPads = new LinkedList<>();

    final VerticalMap<Vertical> scope;

    final double sampleBoost;

    final int recursionDepth;

    boolean isAscendRandomly = false;

    /**
     * Keeps track of whether the initial launchpads have been set up already.
     */
    boolean isInitialized = false;

    /**
     * Create a new top-level instance.
     *
     * @param strategy defines the search within the new instance
     */
    public SearchSpace(int id, DependencyStrategy strategy, RelationSchema schema, Comparator<? super DependencyCandidate> dependencyCandidateComparator) {
        this(id, strategy, null, new SynchronizedVerticalMap<>(schema), schema, dependencyCandidateComparator, 0, 1d);
    }

    /**
     * Create a new sub-level instance.
     *
     * @param strategy defines the search within the new instance
     */
    public SearchSpace(int id, DependencyStrategy strategy,
                       VerticalMap<Vertical> scope, VerticalMap<VerticalInfo> globalVisitees,
                       RelationSchema schema,
                       Comparator<? super DependencyCandidate> dependencyCandidateComparator,
                       int recursionDepth, double sampleBoost) {
        this.id = id;
        this.strategy = strategy;
        this.scope = scope;
        this.globalVisitees = globalVisitees;
        this.recursionDepth = recursionDepth;
        this.sampleBoost = sampleBoost;
        this.launchPadIndex = new SynchronizedVerticalMap<>(schema);
        this.launchPads = new TreeSet<>(dependencyCandidateComparator);
    }

    /**
     * Initialize this instance if not done already. Should be invoked before invoking {@link #discover()}.
     */
    public void ensureInitialized() {
        this.strategy.ensureInitialized(this);
    }

    /**
     * This method discovers data dependencies (either keys or FDs).
     *
     * @return whether there where no more processors operating on this instance as of the return from this method
     */
    public void discover() {
        this.discover(null);
    }

    /**
     * This method discovers data dependencies (either keys or FDs).
     *
     * @param localVisitees known (non-)dependencies
     */
    private void discover(VerticalMap<VerticalInfo> localVisitees) {
        while (!this.interruptFlag) {
            final long startMillis = System.currentTimeMillis();
            DependencyCandidate launchPad = this.pollLaunchPad(localVisitees);
            if (localVisitees == null) {
                this.context.profilingData.launchpadMillis.addAndGet(System.currentTimeMillis() - startMillis);
            }
            if (launchPad == null) break;

            // Keep track of the visited dependency candidates to avoid duplicate visits and enable pruning.
            localVisitees = localVisitees != null ? localVisitees : new VerticalMap<>(this.context.getSchema());
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
                // Note: We can remove the launch pad without synchronization.
                if (logger.isTraceEnabled()) {
                    logger.trace("* Removing subset-pruned launch pad {}.", this.strategy.format(launchPad.vertical));
                }
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
                    if (logger.isTraceEnabled()) {
                        logger.trace("* Escaping launchPad {} from: {}",
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
            if (logger.isDebugEnabled())
                logger.debug("Cowardly giving up on {}.", this.strategy.format(launchPad.vertical));
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
                .map(v -> v.without(strategy.getIrrelevantColumns()))
                .collect(Collectors.toList());

        // Calculate the hitting set.
        Collection<Vertical> hittingSet = this.context.getSchema().calculateHittingSet(
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
                },
                this.context.profilingData
        );

        synchronized (this.launchPads) {
            // Create and store the launchPads.
            for (Vertical escaping : hittingSet) {
                Vertical escapedLaunchPadVertical = launchPad.union(escaping);
                DependencyCandidate escapedLaunchPad = strategy.createDependencyCandidate(escapedLaunchPadVertical);
                if (logger.isTraceEnabled()) logger.trace("  Escaped: {}", strategy.format(escapedLaunchPadVertical));
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
            if (isDefer && this.context.configuration.isDeferFailedLaunchPads) {
                this.deferredLaunchPads.add(launchPad);
                if (logger.isTraceEnabled()) logger.trace("Deferred seed.", this.strategy.format(launchPad.vertical));
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

        if (logger.isDebugEnabled())
            logger.debug("===== Ascending from from {} ======", this.strategy.format(launchPad.vertical));

        // Check whether we should resample.
        if (this.strategy.shouldResample(launchPad.vertical, this.sampleBoost)) {
            this.context.createFocusedSample(launchPad.vertical, sampleBoost);
        }

        // Start the ascension towards a dependency or a (local) maximum non-dependency.
        DependencyCandidate traversalCandidate = launchPad;
        double error;
        while (true) {
            if (logger.isTraceEnabled()) logger.trace("-> {}", strategy.format(traversalCandidate.vertical));

            if (this.context.configuration.isCheckEstimates) {
                this.checkEstimate(strategy, traversalCandidate);
            }

            // Primarily, check if we should operate on agree sets or PLIs.
            if (traversalCandidate.isExact()) {
                // If we have an exhaustive sample, we should operate upon the agree sets.
                error = traversalCandidate.error.get();

                // Store the candidate's state.
                final boolean canBeDependency = error <= strategy.maxDependencyError;
                localVisitees.put(traversalCandidate.vertical, new VerticalInfo(
                        canBeDependency,
                        false,
                        error
                ));

                // Check whether to stop the ascension.
                if (canBeDependency) break;

            } else { // We have no exact sample.
                // Are we confident that this is _not_ a dependency?
                if (traversalCandidate.error.getMin() > strategy.maxDependencyError) {
                    // If we are confident that we do not have a key, than we skip the check.
                    if (logger.isTraceEnabled()) logger.trace("   Skipping check for {} (estimated error: {}).",
                            strategy.format(traversalCandidate.vertical),
                            traversalCandidate.error
                    );
                    error = Double.NaN;

                } else {
                    // If we are not confident that we do not have a dependency, then we better check our candidate.
                    error = strategy.calculateError(traversalCandidate.vertical);

                    // Store the candidate's state.
                    localVisitees.put(traversalCandidate.vertical, new VerticalInfo(
                            error <= strategy.maxDependencyError,
                            false,
                            error
                    ));

                    // Check whether to stop the ascension.
                    if (error <= strategy.maxDependencyError) break;

                    // Potentially create a new agree set sample.
                    if (strategy.shouldResample(traversalCandidate.vertical, sampleBoost)) {
                        this.context.createFocusedSample(traversalCandidate.vertical, sampleBoost);
                    }

                }
            }

            // If we did not find a key, we try to discover further up.
            // Check if we can ascend in the first place.
            if (traversalCandidate.vertical.getArity() >= this.context.getRelationData().getNumColumns() - this.strategy.getNumIrrelevantColumns())
                break;

            // Select the most promising direct superset vertical.
            DependencyCandidate nextCandidate = null;
            // Or select a random candidate: We implement this with a size-one reservoir sampling.
            int numSeenElements = this.isAscendRandomly ? 1 : -1;
            for (Column extensionColumn : this.context.getSchema().getColumns()) {
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
                        || numSeenElements != -1 && this.context.random.nextInt(++numSeenElements) == 0) {
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
            if (logger.isTraceEnabled())
                logger.trace("   Hit the ceiling at {}.", strategy.format(traversalCandidate.vertical));
            error = strategy.calculateError(traversalCandidate.vertical);
            if (logger.isTraceEnabled()) logger.trace("   Checking candidate... actual error: {}", error);
        }

        this.context.profilingData.ascensionHeight.addAndGet(traversalCandidate.vertical.getArity() - launchPad.vertical.getArity());
        this.context.profilingData.ascendMillis.addAndGet(System.currentTimeMillis() - _startMillis);
        this.context.profilingData.numAscends.incrementAndGet();

        if (error <= strategy.maxDependencyError) {
            // If we reached a desired key, then we need to minimize it now.
            if (logger.isTraceEnabled())
                logger.trace("   Key peak in climbing phase: e({})={} -> Need to minimize.", traversalCandidate.vertical, error);
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
                if (logger.isDebugEnabled())
                    logger.debug("[---] {} is maximum non-dependency (err={}).", traversalCandidate.vertical, error);
            } else {
                localVisitees.put(traversalCandidate.vertical, VerticalInfo.forNonDependency());
                if (logger.isDebugEnabled())
                    logger.debug("      {} is local-maximum non-dependency (err={}).", traversalCandidate.vertical, error);
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
        if (logger.isDebugEnabled())
            logger.debug("Estimate check for {}. Status: {}, estimate: {}, actual: {}, delta: {}",
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

        if (logger.isDebugEnabled()) logger.debug("===== Trickling down from {} ======", strategy.format(mainPeak));

        // Prepare to collect the (alleged) minimal and maximal (non-)dependencies.
        Set<Vertical> maximalNonDeps = new HashSet<>();
        VerticalMap<VerticalInfo> allegedMinDeps = new VerticalMap<>(this.context.getSchema());

        // TODO: This does not make sense, does it?
//        // Bootstrap the alleged minimum dependencies with known minimum dependencies.
//        for (Map.Entry<Vertical,VerticalInfo> entry : globalVisitees.getSubsetEntries(mainPeak)) {
//            if (entry.getValue().isDependency && entry.getValue().isExtremal) {
//                if (logger.isDebugEnabled()) logger.debug("* Putting known minimum dependency {}.", strategy.format(entry.getKey()));
//                allegedMinDeps.put(entry.getKey(), entry.getValue());
//            }
//        }
//        for (Map.Entry<Vertical,VerticalInfo> entry : localVisitees.getSubsetEntries(mainPeak)) {
//            if (entry.getValue().isDependency && entry.getValue().isExtremal) {
//                allegedMinDeps.put(entry.getKey(), entry.getValue());
//                if (logger.isDebugEnabled()) logger.debug("* Putting known minimum dependency {}.", strategy.format(entry.getKey()));
//            }
//        }

        // The peaks are the points from which we need to trickle down. We start trickling down from the lowest peaks
        // hoping that they are closer to the minimum dependencies.
        // TODO: See whether this really pays off.
        PriorityQueue<DependencyCandidate> peaks = new PriorityQueue<>(DependencyCandidate.arityComparator);
        peaks.add(new DependencyCandidate(mainPeak, new ConfidenceInterval(mainPeakError, mainPeakError), true));

        // Keep track of visited nodes, so as to visit no node twice.
        Set<Vertical> allegedNonDeps = new HashSet<>();

        // Trickle down from all peaks that we have.
        long _lastUpdateMillis = System.currentTimeMillis();
        while (!peaks.isEmpty()) {
            DependencyCandidate peak = peaks.peek();

            if (System.currentTimeMillis() - _lastUpdateMillis > 1000L) {
                if (logger.isDebugEnabled())
                    logger.debug("--- {} peaks ({})", peaks.size(), formatArityHistogram(peaks.stream().map(dc -> dc.vertical).collect(Collectors.toList())));
                if (logger.isDebugEnabled())
                    logger.debug("    {} alleged minimum deps ({})", allegedMinDeps.size(), formatArityHistogram(allegedMinDeps));
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
                        this.context.getSchema().calculateHittingSet(subsetDeps, null, this.context.profilingData).stream()
                                .map(peak.vertical::without)
                                .collect(Collectors.toList());

                // For escaped peaks, we want to make sure that we deem them to be dependencies.
                // Otherwise, we may discard them.
                for (Vertical escapedPeakVertical : escapedPeakVerticals) {
                    if (escapedPeakVertical.getArity() > 0 && !allegedNonDeps.contains(escapedPeakVertical)) {
                        DependencyCandidate escapedPeak = strategy.createDependencyCandidate(escapedPeakVertical);

                        if (escapedPeak.error.getMean() > strategy.minNonDependencyError) {
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
            } else {
                this.context.profilingData.trickleDepth.addAndGet(mainPeak.getArity() - allegedMinDep.getArity());
            }
        } // hypothesize minimum dependencies
        if (logger.isDebugEnabled())
            logger.debug("* {} alleged minimum dependencies ({})", allegedMinDeps.size(), formatArityHistogram(allegedMinDeps));

        // Register already-known-to-be-minimal dependencies.
        int numUncertainMinDeps = 0;
        for (Map.Entry<Vertical, VerticalInfo> entry : allegedMinDeps.entrySet()) {
            Vertical allegedMinDep = entry.getKey();
            VerticalInfo info = entry.getValue();
            if (info.isExtremal && !globalVisitees.containsKey(allegedMinDep)) {
                if (logger.isDebugEnabled()) logger.debug("[{}] Minimum dependency: {} (error={})",
                        recursionDepth, strategy.format(allegedMinDep), info.error
                );
                globalVisitees.put(allegedMinDep, info);
                strategy.registerDependency(allegedMinDep, info.error, this.context);
            }
            if (!info.isExtremal) numUncertainMinDeps++;
        }

        // NB: We must NOT stop if all dependencies are known to be minimal because they might not be complete!

        if (logger.isDebugEnabled()) logger.debug("* {}/{} alleged minimum dependencies might be non-minimal.",
                numUncertainMinDeps, allegedMinDeps.size()
        );

        // We have an initial hypothesis about where the minimal dependencies are.
        // Now, we determine the corresponding maximal non-dependencies.
        List<Vertical> allegedMaxNonDeps = this.context.getSchema()
                .calculateHittingSet(allegedMinDeps.keySet(), null, this.context.profilingData)
                .stream()
                .map(minLeaveOutVertical -> minLeaveOutVertical.invert(mainPeak))
                .collect(Collectors.toList());
        if (logger.isDebugEnabled())
            logger.debug("* {} alleged maximum non-dependencies ({})", allegedMaxNonDeps.size(), formatArityHistogram(allegedMaxNonDeps));

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
            boolean isNonDep = error > strategy.minNonDependencyError;
            if (logger.isTraceEnabled())
                logger.trace("* Alleged maximal non-dependency {}: non-dep?: {}, error: {}",
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
                peaks.add(new DependencyCandidate(allegedMaxNonDep, new ConfidenceInterval(error, error), true));
            }
        }

        if (peaks.isEmpty()) {
            // When all maximal non-dependency candidates are correct (and, thus, we have no more peaks), we are done.
            // We only need to register the previously uncertain minimum dependencies.
            for (Map.Entry<Vertical, VerticalInfo> entry : allegedMinDeps.entrySet()) {
                Vertical allegedMinDep = entry.getKey();
                VerticalInfo info = entry.getValue();
                if (!info.isExtremal && !globalVisitees.containsKey(allegedMinDep)) {
                    if (logger.isDebugEnabled()) logger.debug("[{}] Minimum dependency: {} (error={})",
                            recursionDepth, strategy.format(allegedMinDep), info.error
                    );
                    info.isExtremal = true;
                    globalVisitees.put(allegedMinDep, info);
                    strategy.registerDependency(allegedMinDep, info.error, this.context);
                }
            }

        } else {
            // Otherwise, we have to continue our search.
            // For that matter, we restrict the search space and re-start the discovery there.
            this.context.profilingData.numMisestimations.incrementAndGet();
            if (logger.isDebugEnabled())
                logger.debug("* {} new peaks ({}).", peaks.size(), formatArityHistogram(peaks.stream().map(dc -> dc.vertical).collect(Collectors.toList())));

            // Define the upper bound for the following dependency search: all the alleged minimal dependencies.
            VerticalMap<Vertical> newScope = new VerticalMap<>(this.context.getSchema());
            for (DependencyCandidate peak : peaks) {
                newScope.put(peak.vertical, peak.vertical);
            }

            // We did not do a good enough job regarding the estimation. Therefore, increase the sampling size.
            double newSampleBoost = sampleBoost * this.sampleBoost;
            if (logger.isDebugEnabled()) logger.debug("* Increasing sampling boost factor to {}.", newSampleBoost);

            SearchSpace nestedSearchSpace = new SearchSpace(-1, this.strategy,
                    newScope,
                    this.globalVisitees,
                    this.context.getSchema(),
                    this.launchPads.comparator(),
                    this.recursionDepth + 1,
                    this.sampleBoost * this.context.configuration.sampleBooster
            );
            nestedSearchSpace.setContext(this.context);

            // Define the lower bound for the following dependency search.
            Set<Column> scopeColumns = newScope.keySet().stream()
                    .flatMap(vertical -> Arrays.stream(vertical.getColumns()))
                    .collect(Collectors.toSet());
            for (Column scopeColumn : scopeColumns) {
                nestedSearchSpace.addLaunchPad(strategy.createDependencyCandidate(scopeColumn));
            }
            long _breakStartMillis = System.currentTimeMillis();
            nestedSearchSpace.discover(localVisitees);
            long recursionMillis = System.currentTimeMillis() - _breakStartMillis;
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            int recursionDepth = 0;
            for (StackTraceElement stackTraceElement : stackTrace) {
                if (stackTraceElement.getMethodName().equals("trickleDown")) recursionDepth++;
            }
            if (recursionDepth == 1) {
                this.context.profilingData.recursionMillis.addAndGet(recursionMillis);
            }
            _startMillis += recursionMillis; // Act as if we started later...

            // Finally, we need to check whether some of our alleged minimal dependencies are actually minimal.
            // We have to check them, because they lie outside of the scope and, thus, cannot be discovered in the
            // above recursion.
            for (Map.Entry<Vertical, VerticalInfo> entry : allegedMinDeps.entrySet()) {
                Vertical allegedMinDep = entry.getKey();
                VerticalInfo info = entry.getValue();
                if (!isImpliedByMinDep(allegedMinDep, globalVisitees)) {
                    if (logger.isDebugEnabled())
                        logger.debug("[{}] Minimum dependency: {} (error={}) (was right after all)",
                                recursionDepth, strategy.format(allegedMinDep), info.error
                        );
                    info.isExtremal = true;
                    globalVisitees.put(allegedMinDep, info);
                    strategy.registerDependency(allegedMinDep, info.error, this.context);
                }
            }
        }

        this.context.profilingData.trickleDownMillis.addAndGet(System.currentTimeMillis() - _startMillis);
        this.context.profilingData.numTrickleDowns.incrementAndGet();
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
        assert minDepCandidate.error.getMin() <= strategy.maxDependencyError;

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
                if (parentCandidate.error.getMin() > strategy.minNonDependencyError) {
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
                    minDepCandidate = new DependencyCandidate(minDepCandidate.vertical, new ConfidenceInterval(error, error), true);
                    if (error > strategy.minNonDependencyError) break;
                }
            }
        }

        // At this point, we deem none of the parents to be dependencies.
        // Still, we need to check if our candidate is an actual dependency.
        double candidateError = minDepCandidate.isExact() ?
                minDepCandidate.error.get() :
                strategy.calculateError(minDepCandidate.vertical);
        if (candidateError <= strategy.maxDependencyError) {
            // TODO: I think, we don't need to add the dep to the localVisitees, because we won't visit it anymore.
            if (logger.isTraceEnabled())
                logger.trace("* Found {}-ary minimum dependency candidate: {}", minDepCandidate.vertical.getArity(), minDepCandidate);
            allegedMinDeps.removeSupersetEntries(minDepCandidate.vertical);
            allegedMinDeps.put(minDepCandidate.vertical, new VerticalInfo(true, areAllParentsKnownNonDeps, candidateError));
            if (areAllParentsKnownNonDeps && this.context.configuration.isCheckEstimates) {
                requireMinimalDependency(strategy, minDepCandidate.vertical);
            }
            return minDepCandidate.vertical;

        } else {
            if (logger.isTraceEnabled())
                logger.trace("* Guessed incorrect {}-ary minimum dependency candidate.", minDepCandidate.vertical.getArity());
            localVisitees.put(minDepCandidate.vertical, new VerticalInfo(false, false));

            // If we had a wrong guess, we re-sample so as to provide insights for the child vertices.
            if (strategy.shouldResample(minDepCandidate.vertical, boostFactor)) {
                this.context.createFocusedSample(minDepCandidate.vertical, boostFactor);
            }
            return null;
        }
    }

    @Override
    public String toString() {
        return String.format("%s[%d, %s]", this.getClass().getSimpleName(), this.id, this.strategy);
    }

    /**
     * Debug method to verify whether error estimates are correct. In particular, exact estimates must be correct.
     */
    private static void requireMinimalDependency(DependencyStrategy strategy, Vertical minDependency) {
        double error = strategy.calculateError(minDependency);
        if (error > strategy.maxDependencyError) {
            throw new AssertionError(String.format("%s should be a minimal dependency but has an error of %f.",
                    strategy.format(minDependency), error
            ));
        }
        if (minDependency.getArity() > 1) {
            for (Vertical parent : minDependency.getParents()) {
                double parentError = strategy.calculateError(parent);
                if (parentError <= strategy.minNonDependencyError) {
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
     * Format a histogram of the arities of {@link Vertical}s contained in the given {@link VerticalMap}.
     *
     * @param verticalMap the {@link VerticalMap}
     * @return the histogram {@link String}
     */
    private static String formatArityHistogram(VerticalMap<?> verticalMap) {
        return formatArityHistogram(verticalMap.keySet());
    }

    /**
     * Format a histogram of the arities of {@link Vertical}s contained in the given {@link Collection}.
     *
     * @param verticals the {@link Collection}
     * @return the histogram {@link String}
     */
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
     * The {@link ProfilingContext} is transient. Hence, when this instance has undergone serialization, e.g., because
     * it was moved over the network, then it must be re-set.
     *
     * @param context the new {@link ProfilingContext}
     */
    public void setContext(ProfilingContext context) {
        this.context = context;
        this.strategy.context = context;
    }

    /**
     * Check whether the interrupt flag has been set on this instance.
     *
     * @return whether the interrupt flag has been set
     */
    public boolean isInterruptFlagSet() {
        return this.interruptFlag;
    }

    /**
     * Set or clear the interrupt flag.
     *
     * @param interruptFlag whether the interrupt flag should be set or not
     */
    public void setInterruptFlag(boolean interruptFlag) {
        this.interruptFlag = interruptFlag;
    }

    /**
     * Tells whether there are launchpads in this search space.
     *
     * @return whether there are launchpads
     */
    public boolean hasLaunchpads() {
        try {
            this.launchPadIndexLock.lock();
            return !this.launchPads.isEmpty();
        } finally {
            this.launchPadIndexLock.unlock();
        }
    }

}
