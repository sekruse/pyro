package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.AgreeSetSample;
import de.hpi.isg.pyro.util.ConfidenceInterval;
import de.hpi.isg.pyro.util.PFDRater;
import de.hpi.isg.pyro.util.PositionListIndex;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import static de.hpi.isg.pyro.util.PositionListIndex.singletonValueId;

/**
 * {@link DependencyStrategy} implementation for partial FDs.
 */
public class FdG1Strategy extends DependencyStrategy {

    private final Column rhs;

    public FdG1Strategy(Column rhs, double maxError) {
        super(maxError);
        this.rhs = rhs;
    }

    @Override
    synchronized public void ensureInitialized(SearchSpace searchSpace) {
        // We better do this thread-safe just in case.
        if (searchSpace.isInitialized) return;

        // We only add a candidate for the 0-ary FD []->RHS.
        double zeroFdError = this.calculateError(this.context.relationData.getSchema().emptyVertical);
        searchSpace.addLaunchPad(new DependencyCandidate(
                this.context.relationData.getSchema().emptyVertical,
                new ConfidenceInterval(zeroFdError, zeroFdError),
                null
        ));

        searchSpace.isInitialized = true;
    }

    @Override
    double calculateError(Vertical fdCandidate) {
        // Special case: Check 0-ary FD.
        if (fdCandidate.getArity() == 0) {
            PositionListIndex rhsPli = this.context.pliCache.get(this.rhs);
            assert rhsPli != null;
            return this.calculateG1(rhsPli.getNip());
        }

        PositionListIndex pli = this.context.pliCache.getOrCreateFor(fdCandidate);
        return this.calculateG1(pli);
    }

    private double calculateG1(PositionListIndex lhsPli) {
        long pliNanos = System.nanoTime();
        long numViolations = 0L;
        final Int2IntOpenHashMap valueCounts = new Int2IntOpenHashMap();
        valueCounts.defaultReturnValue(0);
        final int[] probingTable = this.context.relationData.getColumnData(this.rhs.getIndex()).getProbingTable();

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
        // TODO: Where is the profiling data?
//        pliNanos = System.nanoTime() - pliNanos;
//        this.context._profilingData.probingNanos.addAndGet(pliNanos);
//        this.context._profilingData.numProbings.incrementAndGet();

        return this.calculateG1(numViolations);
    }

    private double calculateG1(double numViolatingTuplePairs) {
        double g1 = numViolatingTuplePairs / this.context.relationData.getNumTuplePairs();
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
        if (this.context.agreeSetSamples == null) {
            return new DependencyCandidate(vertical, new ConfidenceInterval(0, 1), null);
        }

        // Find the best available correlation provider.
        AgreeSetSample agreeSetSample = this.context.getAgreeSetSample(vertical);
        ConfidenceInterval numViolatingTuplePairs = agreeSetSample
                .estimateMixed(vertical, this.rhs, this.context.configuration.estimateConfidence)
                .multiply(this.context.relationData.getNumTuplePairs());

        ConfidenceInterval g1 = this.calculateG1(numViolatingTuplePairs);
        return new DependencyCandidate(vertical, g1, agreeSetSample);
    }

    @Override
    String format(Vertical vertical) {
        return String.format("%s\u2192%s", vertical, this.rhs);
    }

    @Override
    void registerDependency(Vertical vertical, double error, DependencyConsumer discoveryUnit) {
        // TODO: Calculate score.
        discoveryUnit.registerFd(vertical, this.rhs, error, Double.NaN);
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
    public Vertical getIrrelevantColumns() {
        return this.rhs;
    }

    @Override
    public String toString() {
        return String.format("FD[RHS=%s, g1\u2264%.3f]", this.rhs.getName(), this.maxError);
    }
}
