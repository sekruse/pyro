package de.hpi.isg.pyro.significance.metrics;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.significance.CorrelationMetric;

/**
 * Measures the number of overlapping equality pairs of two columns.
 */
public class EqualityPairOverlap implements CorrelationMetric {

    @Override
    public double evaluate(Column a, Column b, Vertical ab, Relation r) {
        return ab.getNep();
    }

    @Override
    public double getLowerBound(Column a, Column b, Relation r) {
        return Math.max(0, a.getNep() + b.getNep() - r.getMaximumNip());
    }

    @Override
    public double getUpperBound(Column a, Column b, Relation r) {
        return Math.min(a.getNep(), b.getNep());
    }
}
