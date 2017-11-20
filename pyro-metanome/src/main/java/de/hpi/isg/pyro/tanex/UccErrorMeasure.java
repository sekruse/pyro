package de.hpi.isg.pyro.tanex;

import de.hpi.isg.pyro.model.RelationData;
import de.hpi.isg.pyro.util.PositionListIndex;

/**
 * Describes a measure to quantify UCC errors.
 */
@FunctionalInterface
public interface UccErrorMeasure {

    double calculateUccError(PositionListIndex pli, RelationData relationData);

    UccErrorMeasure g1Prime = (pli, relationData) -> pli.getNep() / relationData.getNumTuplePairs();

}
