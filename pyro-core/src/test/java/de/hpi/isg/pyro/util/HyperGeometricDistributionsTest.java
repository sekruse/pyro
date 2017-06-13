package de.hpi.isg.pyro.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite for the {@link HyperGeometricDistributions} class.
 */
public class HyperGeometricDistributionsTest {

    @Test
    public void testOverlapProbability() {
        Assert.assertEquals(
                1,
                HyperGeometricDistributions.pmf(0, 4, 0, 10),
                0
        );
        Assert.assertEquals(
                1,
                HyperGeometricDistributions.pmf(4, 0, 0, 10),
                0
        );

        Assert.assertEquals(
                .4d,
                HyperGeometricDistributions.pmf(1, 4, 1, 10),
                0.000000001
        );

        Assert.assertTrue(HyperGeometricDistributions.pmf(100, 200, 100, 1000) < 0.001);
        Assert.assertTrue(HyperGeometricDistributions.pmf(100, 200, 10, 1000) >= 0.000);

    }


}
