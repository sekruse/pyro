package de.hpi.isg.pyro.ducc_dfd;

import de.hpi.isg.pyro.core.SearchSpace;

import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Profiling data for A-DUCC/DFD executions.
 */
public class ProfilingData {
    public final AtomicLong initializationMillis = new AtomicLong(0L);
    public final AtomicLong walkMillis = new AtomicLong(0L);
    public final AtomicLong holeNanos = new AtomicLong(0L);
    public final AtomicLong operationMillis = new AtomicLong(0L);

    public final AtomicLong numWalks = new AtomicLong(0L);
    public final AtomicLong numHoles = new AtomicLong(0L);

    public final AtomicLong hittingSetNanos = new AtomicLong(0L);
    public final AtomicLong errorCalculationNanos = new AtomicLong(0L);

    public final AtomicLong numHittingSets = new AtomicLong(0L);
    public final AtomicLong numErrorCalculations = new AtomicLong(0L);

    public final AtomicLong numDependencies = new AtomicLong(0L);
    public final AtomicLong dependencyArity = new AtomicLong(0L);

    public final Map<String, AtomicLong> searchSpaceMillis = Collections.synchronizedMap(new HashMap<>());

    public void printReport(String title, PrintStream out) {
        out.printf("=======================================================================================\n");
        out.printf("Report for %s\n", title);
        out.printf("---Phases------------------------------------------------------------------------------\n");
        out.printf("Initialization:                                                  %,10.3f s (%.2f%%)\n", initializationMillis.get() / 1000d, getRuntimePercentage(initializationMillis.get()));
        out.printf("Random walks:                                                    %,10.3f s (%.2f%%)\n", walkMillis.get() / 1000d, getRuntimePercentage(walkMillis.get()));
        out.printf("Hole searches:                                                   %,10.3f s (%.2f%%)\n", holeNanos.get() / 1e9, getRuntimePercentage(holeNanos.get() * 1e-6));
        out.printf("Total:                                                           %,10.3f s\n", (initializationMillis.get() + operationMillis.get()) / 1000d);
        out.printf("- -Counts- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n");
        out.printf("Random walks:                                                    %,10d #\n", numWalks.get());
        out.printf("Holes searches:                                                  %,10d #\n", numHoles.get());
        out.printf("---Operations--------------------------------------------------------------------------\n");
        out.printf("Error calculation:                                               %,10.3f s (%.2f%%)\n", errorCalculationNanos.get() / 1e9d, getRuntimePercentage(errorCalculationNanos.get() * 1e-6));
        out.printf("Hitting sets:                                                    %,10.3f s (%.2f%%)\n", hittingSetNanos.get() / 1e9d, getRuntimePercentage(hittingSetNanos.get() * 1e-6));
        out.printf("- -Counts- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - \n");
        out.printf("Error calculation:                                               %,10d #\n", numErrorCalculations.get());
        out.printf("Hitting sets:                                                    %,10d #\n", numHittingSets.get());
        out.printf("---Miscellaneous-----------------------------------------------------------------------\n");
        out.printf("Dependencies:                                                    %,10d #\n", numDependencies.get());
        out.printf("Arity:                                                           %,10.3f\n", dependencyArity.get() / (double) numDependencies.get());
        out.printf("Error calculation efficiency:                                    %,10.3f ms/calculation\n", errorCalculationNanos.get() / numDependencies.doubleValue() * 1e-6);
        out.printf("---Search spaces-----------------------------------------------------------------------\n");
        List<Map.Entry<String, AtomicLong>> searchSpaceMillisRanking = new ArrayList<>(searchSpaceMillis.entrySet());
        searchSpaceMillisRanking.sort(Comparator.comparingLong(e -> -e.getValue().get()));
        for (Map.Entry<String, AtomicLong> entry : searchSpaceMillisRanking) {
            String key = entry.getKey();
            out.printf("%-64s %,10.3f s (%.2f%%)\n",
                    key.substring(0, Math.min(63, key.length())) + ":",
                    entry.getValue().get() / 1000d,
                    getRuntimePercentage(entry.getValue().get())
            );

        }
        out.printf("=======================================================================================\n");
    }

    private double getRuntimePercentage(double millis) {
        return 100d * millis / (initializationMillis.get() + operationMillis.get());
    }

}
