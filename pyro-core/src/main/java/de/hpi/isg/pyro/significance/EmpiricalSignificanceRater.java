package de.hpi.isg.pyro.significance;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;
import de.hpi.isg.pyro.significance.histograms.HistogramBuilder;

/**
 * Utility to rate the significance of a column pair empirically.
 */
public class EmpiricalSignificanceRater {

    private final double originalValue;

    private final CorrelationMetric metric;

    private final HistogramBuilder histogramBuilder;

    public EmpiricalSignificanceRater(CorrelationMetric metric, Column a, Column b, Vertical ab, Relation r) {
        this.metric = metric;
        this.originalValue = this.metric.evaluate(a, b, ab, r);
        this.histogramBuilder = new HistogramBuilder();
    }

    public void includeRandomSpecimen(Column a, Column b, Vertical ab, Relation r) {
        double value = this.metric.evaluate(a, b, ab, r);
        this.histogramBuilder.add(value);
    }

    @Override
    public String toString() {
        return "EmpiricalSignificanceRater[" + this.metric.getClass().getSimpleName() + ']';
    }

    public double getLeftTailbound() {
        return this.histogramBuilder.getLeftTailbound(this.getOriginalValue());
    }

    public double getRightTailbound() {
        return this.histogramBuilder.getRightTailbound(this.getOriginalValue());
    }

    public double getOriginalValue() {
        return this.originalValue;
    }

    public void includeAll(EmpiricalSignificanceRater that) {
        this.histogramBuilder.addAll(that.histogramBuilder);
    }
}
