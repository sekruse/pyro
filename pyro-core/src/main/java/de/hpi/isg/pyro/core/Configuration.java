package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.properties.MetanomeProperty;

/**
 * Describes the configuration of Pyro.
 */
public class Configuration extends AbstractPFDConfiguration {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Traversal settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    public int parallelism = 0;

    @MetanomeProperty
    public boolean isDeferFailedLaunchPads = true;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sampling settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    public int sampleSize = 1_000;

    @MetanomeProperty
    public double sampleBooster = 10;

    @MetanomeProperty
    public Integer seed = null;

    @MetanomeProperty
    public double estimateConfidence = 0.9;

    @MetanomeProperty
    public int randomAscendThreads = 2;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Cache settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    public boolean isCacheIntermediatePLIs = false;

    @MetanomeProperty
    public boolean isUseWeakReferencesForPlis = true;

    @MetanomeProperty
    public boolean isUseWeakReferencesForSamples = false;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscellaneous settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @MetanomeProperty
    public boolean isPauseInitially = false;

    @MetanomeProperty
    public boolean isCheckEstimates = false;

    /**
     * If Pyro should be run in the distributed mode, then this property should include a semicolon separated list of
     * hosts to run on, e.g, {@code "worker1:35711;worker2:35711;worker3:35711"}, with the first host being the head of
     * the operation (here: {@code worker1:35711}). Otherwise, this property should not be specified.
     */
    @MetanomeProperty
    public String hosts = null;

}
