package de.hpi.isg.pyro.util;

/**
 * Rates a partial functional dependency.
 */
public interface PFDRater {

    /**
     * Removes some trailing digits from an error so as to avoid problems with floating point inaccurracies. This method
     * will only return {@code 0} when the input is {@code 0}.
     *
     * @param error the error to round
     * @return the rounded error
     */
    static double round(double error) {
        return Math.ceil(error * 32768d) / 32768d; // 32768 = 2^15
    }

}
