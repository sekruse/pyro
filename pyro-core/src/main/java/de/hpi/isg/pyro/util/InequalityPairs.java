package de.hpi.isg.pyro.util;

/**
 * This utility helps to perform calculations with inequality pairs.
 */
public class InequalityPairs {

    /**
     * Converts a number of equality pairs to the implied average distinct value rate in some relation.
     *
     * @param nep the number of equality pairs
     * @param r   the number of tuples in the relation
     * @return the average number of uniform distinct values in the relation
     */
    public static double nepToDistinctValues(double nep, double r) {
        return r * r / (2 * nep + r);
    }


    /**
     * Converts a number of inequality pairs to the implied average distinct value rate in some relation.
     *
     * @param nip the number of inequality pairs
     * @param r   the number of tuples in the relation
     * @return the average number of distinct values in the relation divided by the number of tuples
     */
    public static double nipToDistinctValues(double nip, double r) {
        return nepToDistinctValues(nipToNep(nip, r), r);
    }

    public static double nipToNep(double nip, double r) {
        return (r * r - r) / 2 - nip;
    }


    public static double nepToNip(double nep, double r) {
        return nipToNep(nep, r);
    }

    public static double nipToRelationSize(double nip) {
        return (1d + Math.sqrt(8 * nip + 1)) / 2d;
    }

    public static double relationSizeToNip(double r) {
        return r * (r - 1) / 2;
    }

    public static double keyness(double nip, double maxNip) {
//        double r = nipToRelationSize(maxNip);
//        double numUniformDistinctValues = InequalityPairs.nipToDistinctValues(nip, r);
//        return 1 / numUniformDistinctValues;
        return nip / maxNip;
    }

    public static double distinctValuesToNep(double distinctValues, double r) {
        return (r * r) / (2 * distinctValues) - r / 2;
    }

    public static double groupSizeToNep(double groupSize, double r) {
        return r * (groupSize - 1) / 2;
    }

    public static double nepToGroupSize(double nep, double r) {
        double distinctValues = nepToDistinctValues(nep, r);
        return r / distinctValues;
    }

    public static double epPerTuple(double nip, double maxNip) {
        double r = nipToRelationSize(maxNip);
        double nep = nepToNip(nip, r);
        return nep / r;
    }

    public static double ipPerTuple(double nip, double maxNip) {
        double r = nipToRelationSize(maxNip);
        return nip / r;
    }


}
