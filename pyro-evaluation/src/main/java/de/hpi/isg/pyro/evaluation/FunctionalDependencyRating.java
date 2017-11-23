package de.hpi.isg.pyro.evaluation;

import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;

/**
 * Rates a {@link FunctionalDependency} with a relevance.
 */
public class FunctionalDependencyRating {

    private final FunctionalDependency partialFunctionalDependency;

    private final double relevance;

    public FunctionalDependencyRating(FunctionalDependency partialFunctionalDependency, double relevance) {
        this.partialFunctionalDependency = partialFunctionalDependency;
        this.relevance = relevance;
    }

    public FunctionalDependency getFunctionalDependency() {
        return partialFunctionalDependency;
    }

    public double getRelevance() {
        return relevance;
    }

}
