package de.hpi.isg.pyro.significance;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;

/**
 * A metric that describes the correlation of two columns.
 */
public interface CorrelationMetric {

    double evaluate(Column a, Column b, Vertical ab, Relation r);

    double getLowerBound(Column a, Column b, Relation r);

    double getUpperBound(Column a, Column b, Relation r);

}
