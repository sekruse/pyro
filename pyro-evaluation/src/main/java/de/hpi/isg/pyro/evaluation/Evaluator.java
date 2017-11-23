package de.hpi.isg.pyro.evaluation;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import de.hpi.isg.pyro.util.PlainTextFD;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Evaluates partial dependencies.
 */
public class Evaluator {

    private final Map<String, Collection<PlainTextFD>> groundtruth;

    /**
     * Create a new instance with a given groundtruth.
     */
    public Evaluator(Collection<PlainTextFD> groundtruth) {
        this.groundtruth = indexByRhs(groundtruth);
    }

    private static final Map<String, Collection<PlainTextFD>> indexByRhs(Collection<PlainTextFD> fds) {
        Map<String, Collection<PlainTextFD>> index = new HashMap<>();
        for (PlainTextFD fd : fds) {
            index.computeIfAbsent(fd.rhs, key -> new ArrayList<>()).add(fd);
        }
        return index;
    }

    /**
     * Evaluates the given FDs against the groundtruth and prints a summary.
     */
    private void evaluate(List<PlainTextFD> fds, boolean isUseGeneralizedPR, boolean isVerbose) {
        Map<String, Collection<PlainTextFD>> indexedFDs = indexByRhs(fds);

        double truePositives = 0;
        double falseNegatives = 0;

        Set<String> allRhs = new HashSet<>();
        allRhs.addAll(indexedFDs.keySet());
        allRhs.addAll(this.groundtruth.keySet());
        int groundtruthSize = this.groundtruth.values().stream().mapToInt(Collection::size).sum();
        for (String rhs : allRhs) {
            double localTruePositives = 0;
            Collection<PlainTextFD> fdGroup = indexedFDs.getOrDefault(rhs, new ArrayList<>(0));
            Collection<PlainTextFD> trueFdGroup = this.groundtruth.getOrDefault(rhs, new ArrayList<>(0));

            for (PlainTextFD fd : fdGroup) {
                if (trueFdGroup.contains(fd)) {
                    localTruePositives++;
                } else if (isUseGeneralizedPR) {
                    // Generalized PR: Find the true FD with the highest Jaccard similarity. Use that value as partial true positive.
                    double maxJaccardSimilarity = 0d;
                    for (PlainTextFD trueFd : trueFdGroup) {
                        maxJaccardSimilarity = Math.max(maxJaccardSimilarity, calculateJaccardSimilarity(trueFd, fd));
                    }
                    localTruePositives += maxJaccardSimilarity;
                }
            }

            System.out.printf("Results for [*]->%s:\n", rhs);
            System.out.printf("* Precision: %.3f (%,.3f/%,d)\n",
                    precision(localTruePositives, fdGroup.size()),
                    localTruePositives, fdGroup.size()
            );
            System.out.printf("* Recall:    %.3f (%,.3f/%,d)\n",
                    precision(localTruePositives, trueFdGroup.size()),
                    localTruePositives, trueFdGroup.size()
            );
            if (isVerbose) {
                System.out.printf("* Actual FDs: %s\n", trueFdGroup);
                System.out.printf("* Found FDs:  %s\n", fdGroup);
            }

            truePositives += localTruePositives;
        }

        System.out.printf("Overall results:\n");
        System.out.printf("* Precision: %.3f (%,.3f/%,d)\n",
                precision(truePositives, fds.size()), truePositives, fds.size()
        );
        System.out.printf("* Recall:    %.3f (%,.3f/%,d)\n",
                recall(truePositives, groundtruthSize), truePositives, groundtruthSize
        );
    }

    private static double calculateJaccardSimilarity(PlainTextFD fd1, PlainTextFD fd2) {
        // Swap for efficiency.
        if (fd1.lhs.size() > fd2.lhs.size()) return calculateJaccardSimilarity(fd2, fd1);

        // Calculate the intersection size.
        int overlap = 0;
        for (String column : fd1.lhs) {
            if (fd2.lhs.contains(column)) overlap++;
        }

        // Calculate the Jaccard similarity via the inclusion-exclusion principle.
        return overlap / (double) (fd1.lhs.size() + fd2.lhs.size() - overlap);
    }

    private static final double precision(double truePositives, double allPositives) {
        return truePositives / allPositives;
    }

    private static final double recall(double truePositives, double allTrues) {
        return truePositives / allTrues;
    }

    public static void main(String[] args) throws IOException {
        final Parameters parameters = parseParameters(args);

        // Load groundtruth.
        List<PlainTextFD> groundtruth = loadFDs(parameters.groundtruth);
        Evaluator evaluator = new Evaluator(groundtruth);
        System.out.printf("Using %d FDs in %s as groundtruth.\n", groundtruth.size(), parameters.groundtruth);
        System.out.println();

        // Evaluate datasets.
        for (String dataset : parameters.datasets) {
            List<PlainTextFD> fds = loadFDs(dataset);
            System.out.printf("Evaluating %d FDs in %s.\n", fds.size(), dataset);
            evaluator.evaluate(fds, parameters.isUseGeneralizedPR, parameters.isVerbose);
            System.out.println();
        }
    }

    private static Parameters parseParameters(String[] args) {
        final Parameters parameters = new Parameters();
        final JCommander jCommander = new JCommander(parameters);
        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            System.err.println("Could not parse command line args: " + e.getMessage());
            StringBuilder sb = new StringBuilder();
            jCommander.usage(sb);
            System.err.println(sb.toString());
            System.exit(1);
        }
        return parameters;
    }

    private static List<PlainTextFD> loadFDs(String path) throws IOException {
        // Load FDs.
        List<PlainTextFD> fds;
        if (!new File(path).exists()) {
            throw new IllegalArgumentException("No such file: " + path);
        }
        if (path.endsWith(".txt")) {
            fds = PlainTextFD.loadFromTextFile(path);
        } else {
            fds = PlainTextFD.loadFromMetanome(path);
        }

        // Normalize LHS.
        for (PlainTextFD fd : fds) {
            fd.lhs.sort(String::compareTo);
        }
        return fds;
    }

    /**
     * Parameters for this application.
     */
    public static class Parameters {

        @Parameter(description = "files that contain verifyable FDs", required = true, variableArity = true)
        public List<String> datasets = new ArrayList<>();

        @Parameter(names = {"-g", "--groundtruth"}, description = "file that contains the groundtruth of FDs", required = true)
        public String groundtruth;

        @Parameter(names = {"-v", "--verbose"}, description = "verbose output")
        public boolean isVerbose = false;

        @Parameter(names = {"--generalized-pr"}, description = "use generalized precision/recall measures")
        public boolean isUseGeneralizedPR = false;

    }

}
