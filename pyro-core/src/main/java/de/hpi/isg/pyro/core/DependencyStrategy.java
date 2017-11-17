package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.util.AgreeSetSample;
import de.hpi.isg.pyro.util.PositionListIndex;

import java.io.Serializable;

/**
 * A dependency strategy defines how to estimate and evaluate dependency candidates of a certain kind. It furthermore
 * stores the parameters that guide the search.
 */
abstract public class DependencyStrategy implements Serializable {

    /**
     * The maximum error permitted for dependencies.
     */
    final double maxDependencyError;

    /**
     * The maximum error permitted for dependencies.
     */
    final double minNonDependencyError;

    /**
     * The {@link ProfilingContext} on which this instance operates.
     */
    transient ProfilingContext context;

    /**
     * Creates a new instance.
     *
     * @param maxError the maximum permitted error for dependencies
     */
    protected DependencyStrategy(double maxError, double deviation) {
        this.maxDependencyError = maxError + deviation;
        this.minNonDependencyError = maxError - deviation;
    }

    /**
     * Create the initial {@link DependencyCandidate}s for the given {@link SearchSpace} if not done already.
     *
     * @param searchSpace the {@link SearchSpace}
     */
    abstract public void ensureInitialized(SearchSpace searchSpace);

    /**
     * Create a new {@link DependencyCandidate} for the given {@link Vertical} and estimate its error.
     *
     * @param candidate the {@link Vertical}
     * @return the {@link DependencyCandidate}
     */
    abstract DependencyCandidate createDependencyCandidate(Vertical candidate);

    /**
     * Calculate the actual error for a given dependency candidate.
     *
     * @param candidate entails the dependency candidate
     * @return the error
     */
    abstract double calculateError(Vertical candidate);

    /**
     * Format a dependency candidate.
     *
     * @param vertical entails the dependency candidate
     * @return a formatted {@link String}
     */
    abstract String format(Vertical vertical);

    /**
     * Register a dependency.
     *
     * @param vertical      entails the dependency
     * @param error         the calculated error of the dependency
     * @param discoveryUnit that discovered the dependency
     */
    abstract void registerDependency(Vertical vertical, double error, DependencyConsumer discoveryUnit);

    /**
     * Decide whether it is worthwhile to create a focused sampling on the given {@link Vertical}.
     *
     * @param vertical    the {@link Vertical} for which a {@link AgreeSetSample} might be needed
     * @param boostFactor the boost factor that would be used for the resampling
     * @return whether a resampling is recommended
     */
    boolean shouldResample(Vertical vertical, double boostFactor) {
        if (context.configuration.sampleSize <= 0 || vertical.getArity() < 1) return false;

        // Do we have an exact sample already?
        AgreeSetSample currentSample = context.getAgreeSetSample(vertical);
        if (currentSample.isExact()) return false;

        // Get an estimate of the number of equality pairs in the vertical.
        PositionListIndex pli = context.pliCache.get(vertical);
        double nep = pli != null ?
                pli.getNep() :
                currentSample.estimateAgreements(vertical) * context.getRelationData().getNumTuplePairs();

        // Would the new sample be exact?
        if (nep <= context.configuration.sampleSize * boostFactor) return true;

        // Will we achieve an improved sampling ratio?
        double newSamplingRatio = context.configuration.sampleSize * boostFactor / nep;
        return newSamplingRatio >= 2 * currentSample.getSamplingRatio();
    }

    /**
     * Tells whether the {@link Column} with the given {@code columnIndex} is irrelevant to the type of dependency being discovered.
     *
     * @param columnIndex the column index
     * @return whether the {@link Column} is irrelevant
     */
    abstract boolean isIrrelevantColumn(int columnIndex);

    /**
     * Tells whether the given {@link Column} is irrelevant to the type of dependency being discovered.
     *
     * @param column the {@link Column}
     * @return whether the {@link Column} is irrelevant
     */
    boolean isIrrelevantColumn(Column column) {
        return this.isIrrelevantColumn(column.getIndex());
    }

    /**
     * Retrieve the overall number of irrelevant columns in the {@link #context}.
     *
     * @return the number of irrelevant columns
     * @see #getIrrelevantColumns()
     */
    abstract int getNumIrrelevantColumns();

    /**
     * Retrieve the irrelevant columns in the current {@link #context}.
     *
     * @return a {@link Vertical} comprising exactly all irrelevant {@link Column}s
     */
    public abstract Vertical getIrrelevantColumns();

}
