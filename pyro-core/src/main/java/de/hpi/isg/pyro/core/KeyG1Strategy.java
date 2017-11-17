package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.AgreeSetSample;
import de.hpi.isg.pyro.util.ConfidenceInterval;
import de.hpi.isg.pyro.util.PFDRater;
import de.hpi.isg.pyro.util.PositionListIndex;

/**
 * {@link DependencyStrategy} implementation for partial keys.
 */
public class KeyG1Strategy extends DependencyStrategy {

    public KeyG1Strategy(double maxError, double deviation) {
        super(maxError, deviation);
    }

    @Override
    synchronized public void ensureInitialized(SearchSpace searchSpace) {
        // We do this operation thread-safe, just in case.
        if (searchSpace.isInitialized) return;

        // Initialize the launchPads.
        for (Column column : this.context.getSchema().getColumns()) {
            if (this.isIrrelevantColumn(column)) continue;

            // We need to estimate the error of the dependency candidate.
            searchSpace.addLaunchPad(this.createDependencyCandidate(column));
        }

        searchSpace.isInitialized = true;
    }

    @Override
    double calculateError(Vertical keyCandidate) {
        PositionListIndex pli = this.context.pliCache.getOrCreateFor(keyCandidate);
        return this.calculateKeyError(pli);
    }

    private double calculateKeyError(PositionListIndex pli) {
        return this.calculateKeyError(pli.getNep());
    }

    private double calculateKeyError(double numEqualityPairs) {
        double keyError = numEqualityPairs / this.context.getRelationData().getNumTuplePairs();
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
        // If we have columns, there is no need to estimate.
        if (vertical.getArity() == 1) {
            PositionListIndex pli = this.context.pliCache.getOrCreateFor(vertical);
            double keyError = this.calculateKeyError((long) pli.getNep());
            return new DependencyCandidate(vertical, new ConfidenceInterval(keyError, keyError), true);
        }

        if (this.context.agreeSetSamples == null) {
            return new DependencyCandidate(vertical, new ConfidenceInterval(0, 1), false);
        }

        // Find the best available correlation provider.
        AgreeSetSample agreeSetSample = this.context.getAgreeSetSample(vertical);
        ConfidenceInterval estimatedEqualityPairs = agreeSetSample
                .estimateAgreements(vertical, this.context.configuration.estimateConfidence)
                .multiply(this.context.relationData.getNumTuplePairs());
        ConfidenceInterval keyError = this.calculateKeyError(estimatedEqualityPairs);
        return new DependencyCandidate(vertical, keyError, false);
    }

    @Override
    String format(Vertical vertical) {
        return String.format("key(%s)", vertical);
    }

    @Override
    void registerDependency(Vertical vertical, double error, DependencyConsumer discoveryUnit) {
        // TODO: Calculate score.
        discoveryUnit.registerUcc(vertical, error, Double.NaN);
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
    public Vertical getIrrelevantColumns() {
        return this.context.relationData.getSchema().emptyVertical;
    }

    @Override
    public String toString() {
        return String.format("Key[g1\u2264(%.3f..%.3f)]", this.minNonDependencyError, this.maxDependencyError);
    }
}
