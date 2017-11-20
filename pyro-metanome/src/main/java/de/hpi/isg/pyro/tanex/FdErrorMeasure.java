package de.hpi.isg.pyro.tanex;

import de.hpi.isg.pyro.model.ColumnData;
import de.hpi.isg.pyro.model.RelationData;
import de.hpi.isg.pyro.util.PositionListIndex;

/**
 * Describes a measure to quantify FD errors.
 */
public interface FdErrorMeasure {

    double calculateZeroAryFdError(ColumnData rhs, RelationData relationData);

    double calculateFdError(PositionListIndex lhsPli, PositionListIndex jointPli, RelationData relationData);

    FdErrorMeasure g1Prime = new FdErrorMeasure() {
        @Override
        public double calculateZeroAryFdError(ColumnData rhs, RelationData relationData) {
            return 1 - rhs.getPositionListIndex().getNep() / relationData.getNumTuplePairs();
        }

        @Override
        public double calculateFdError(PositionListIndex lhsPli, PositionListIndex jointPli, RelationData relationData) {
            return (lhsPli.getNepAsLong() - jointPli.getNepAsLong()) / (double) relationData.getNumTuplePairs();
        }
    };

}
