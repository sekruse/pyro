package de.hpi.isg.pyro.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link InequalityPairs} class.
 */
public class InequalityPairsTest {

    @Test
    public void testNipToRelation() {
        for (int i = 0; i < 10000; i++) {
            double r = InequalityPairs.nipToRelationSize(i);
            Assert.assertEquals(i, r * (r - 1) / 2, 0.00001);
        }
    }

    @Test
    public void testKeyness() {
        double maxNip = 1000 * 999 / 2;
        for (int i = 1; i < 1001; i++) {
            double f = 1000d / i;
            double nip = maxNip - i * (f * (f - 1d) / 2d);
            double nipRatio = nip / maxNip;
            double keyness = InequalityPairs.keyness(nip, maxNip);
            System.out.printf("%.3f -> %.3f\n", nipRatio, keyness);
        }
    }

}