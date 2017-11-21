package de.hpi.isg.pyro.core;

import com.beust.jcommander.Parameter;
import de.hpi.isg.pyro.properties.MetanomeProperty;

/**
 * Describes the configuration of Pyro.
 */
public class Configuration extends AbstractPFDConfiguration {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Error settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Parameter(names = "--error-dev", description = "allowed deviation of error")
    @MetanomeProperty
    public double errorDev = 0d;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Traversal settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Parameter(names = "--parallelism", description = "parallelism to use on each node running Pyro or 0 to use all processors")
    @MetanomeProperty
    public int parallelism = 0;

    @Parameter(names = "--maxThreadsPerSearchSpace", description = "maximum number threads jointly working on a search space")
    @MetanomeProperty
    public int maxThreadsPerSearchSpace = -1;

    @Parameter(names = "--isDeferFailedLaunchPads", description = "whether to defer launchpads not yielding a dependency")
    @MetanomeProperty
    public boolean isDeferFailedLaunchPads = true;

    @Parameter(names = "--launchpadOrder", description = "how to prioritize launchpads (arity, error)")
    @MetanomeProperty
    public String launchpadOrder = "error";

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Sampling settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Parameter(names = "--sampleSize", description = "initial size of agree set samples")
    @MetanomeProperty
    public int sampleSize = 1_000;

    @Parameter(names = "--sampleBooster", description = "boost of agree set sample size in nested search spaces")
    @MetanomeProperty
    public double sampleBooster = 10;

    @Parameter(names = "--seed", description = "a seed to use to provide randomness")
    @MetanomeProperty
    public Integer seed = null;

    @Parameter(names = "--estimateConfidence", description = "p-value for confidence intervals")
    @MetanomeProperty
    public double estimateConfidence = 0.9;

    @Parameter(names = "--randomAscendThreads", description = "number of threads on a single search space that cause a switch to a random ascend strategy")
    @MetanomeProperty
    public int randomAscendThreads = 2;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Cache settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Parameter(names = "--cachingProbability", description = "the probability of caching a calculated PLI")
    @MetanomeProperty
    public double cachingProbability = 0.5d;

    @Parameter(names = "--naryIntersectionSize", description = "number of PLIs that should be intersected in a single operation")
    @MetanomeProperty
    public int naryIntersectionSize = 4;


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscellaneous settings.
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Parameter(names = "--isCheckEstimates", description = "whether to check error estimates for correctness")
    @MetanomeProperty
    public boolean isCheckEstimates = false;

    @Parameter(names = "--isInitialPause", description = "whether to pause before starting the algorithm")
    @MetanomeProperty
    public boolean isInitialPause = false;

//    /**
//     * If Pyro should be run in the distributed mode, then this property should include a semicolon separated list of
//     * hosts to run on, e.g, {@code "worker1:35711;worker2:35711;worker3:35711"}, with the first host being the head of
//     * the operation (here: {@code worker1:35711}). Otherwise, this property should not be specified.
//     */
//    @MetanomeProperty
//    public String hosts = null;

}
