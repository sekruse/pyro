package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.AgreeSetSample;
import de.hpi.isg.pyro.util.ConfidenceInterval;
import de.hpi.isg.pyro.util.PFDRater;
import de.hpi.isg.pyro.util.PositionListIndex;

/**
 * {@link DependencyStrategy} implementation for partial keys.
 */
public class KeyG1Strategy extends DependencyStrategy {

    public KeyG1Strategy(double maxError) {
        super(maxError);
    }

    @Override
    double calculateError(Vertical keyCandidate) {
        PositionListIndex pli = keyCandidate.getPositionListIndex(this.context.pliCache);
        return this.calculateKeyError(pli);
    }

    private double calculateKeyError(PositionListIndex pli) {
        return this.calculateKeyError(pli.getNep());
    }

    private double calculateKeyError(double numEqualityPairs) {
        double keyError = numEqualityPairs / this.context.getRelation().getNumTuplePairs();
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
            double keyError = this.calculateKeyError((long) vertical.getNep());
            return new DependencyCandidate(vertical, new ConfidenceInterval(keyError, keyError), null);
        }

        if (this.context.agreeSetSamples == null) {
            return new DependencyCandidate(vertical, new ConfidenceInterval(0, 1), null);
        }

        // Find the best available correlation provider.
        AgreeSetSample agreeSetSample = this.context.getAgreeSetSample(vertical);
        ConfidenceInterval estimatedEqualityPairs = agreeSetSample
                .estimateAgreements(vertical, this.context.configuration.estimateConfidence)
                .multiply(this.context.relation.getNumTuplePairs());
        ConfidenceInterval keyError = this.calculateKeyError(estimatedEqualityPairs);
        return new DependencyCandidate(vertical, keyError, agreeSetSample);
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
        return this.context.relation.emptyVertical;
    }

    @Override
    public String toString() {
        return String.format("Key[g1\u2264%.3f]", this.maxError);
    }
}
