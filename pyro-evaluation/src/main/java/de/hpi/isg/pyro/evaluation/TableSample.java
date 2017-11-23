package de.hpi.isg.pyro.evaluation;

import de.hpi.isg.mdms.clients.location.CsvFileLocation;
import de.hpi.isg.mdms.domain.constraints.FunctionalDependency;
import de.hpi.isg.mdms.model.MetadataStore;
import de.hpi.isg.mdms.model.location.Location;
import de.hpi.isg.mdms.model.targets.Table;
import de.hpi.isg.mdms.model.targets.Target;
import de.hpi.isg.mdms.model.util.IdUtils;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.FileInputGenerator;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * This class stores a sample of tables and can answer some kind of queries on that sample.
 */
public class TableSample {

    private final Table table;

    private final ArrayList<List<String>> sample;

    private final IdUtils idUtils;

    /**
     * Create a new instance.
     *
     * @param table that should be loaded and sampled
     * @param size  the maximum sample size
     * @return the new instance
     * @throws IOException if the table could not be loaded properly
     */
    public static TableSample load(MetadataStore store, Table table, int size) throws IOException {
        // Configure the file input.
        Location location = table.getLocation();
        if (!(location instanceof CsvFileLocation)) {
            System.err.printf("Cannot load table %s: It's location is of type %s.\n",
                    table.getName(),
                    table.getLocation().getClass()
            );
            return null;
        }
        CsvFileLocation csvFileLocation = (CsvFileLocation) location;
        File file;
        try {
            file = new File(new URI(csvFileLocation.getPath()));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
        ConfigurationSettingFileInput fileInputSettings = new ConfigurationSettingFileInput(file.getPath());
        fileInputSettings.setSeparatorChar(String.valueOf(csvFileLocation.getFieldSeparator()));
        fileInputSettings.setQuoteChar(String.valueOf(csvFileLocation.getQuoteChar()));
        fileInputSettings.setNullValue(csvFileLocation.getNullString());
        fileInputSettings.setSkipDifferingLines(true);
        FileInputGenerator fileInputGenerator;
        try {
            fileInputGenerator = new DefaultFileInputGenerator(fileInputSettings);
        } catch (AlgorithmConfigurationException e) {
            throw new IOException(e);
        }
        RelationalInput input;
        try {
            input = fileInputGenerator.generateNewCopy();
        } catch (InputGenerationException | AlgorithmConfigurationException e) {
            throw new IOException(e);
        }

        // Apply reservoir sampling to the file contents.
        Random random = new Random();
        int numSeenRows = 0;
        ArrayList<List<String>> sample = new ArrayList<>();
        try {
            while (input.hasNext()) {
                List<String> row = input.next();
                numSeenRows++;
                if (numSeenRows <= size) {
                    sample.add(row);
                } else {
                    int randomIndex = random.nextInt(numSeenRows);
                    if (randomIndex < sample.size()) sample.set(randomIndex, row);
                }
            }
        } catch (InputIterationException e) {
            throw new IOException(e);
        }

        return new TableSample(table, sample, store.getIdUtils());
    }

    private TableSample(Table table, ArrayList<List<String>> sample, IdUtils idUtils) {
        this.table = table;
        this.sample = sample;
        this.idUtils = idUtils;
    }

    /**
     * Retrieves the top k theories and frequencies of some {@link FunctionalDependency}.
     * @param fd the {@link FunctionalDependency}
     * @param numTheories the number of theories to retrieve
     * @return {@link TheoryCounter}s for the top k theories; sorted ascendingly by count
     */
    public ArrayList<TheoryCounter> getTopTheories(FunctionalDependency fd, int numTheories) {
        // Go over the sample and count the number of occurrences for each theory.
        Object2IntOpenHashMap<Theory> theoryCounter = new Object2IntOpenHashMap<>();
        theoryCounter.defaultReturnValue(0);
        for (List<String> row : this.sample) {
            Theory theory = this.createTheory(fd, row);
            theoryCounter.addTo(theory, 1);
        }

        // Retain the top-k theories using a priority queue.
        PriorityQueue<TheoryCounter> topTheories = new PriorityQueue<>(numTheories, Comparator.comparingInt(TheoryCounter::getCounter));
        for (Object2IntMap.Entry<Theory> entry : theoryCounter.object2IntEntrySet()) {
            // Find out whether to keep the theory.
            boolean shouldInsert;
            if (topTheories.size() < numTheories) shouldInsert = true;
            else if (shouldInsert = topTheories.peek().counter < entry.getIntValue()) {
                topTheories.poll();
            }

            // Keep it if so.
            if (shouldInsert) topTheories.add(new TheoryCounter(entry.getKey(), entry.getIntValue()));
        }

        // Sorted insertion of top theories into a list.
        ArrayList<TheoryCounter> result = new ArrayList<>(topTheories.size());
        while (!topTheories.isEmpty()) result.add(topTheories.poll());

        // Count the LHS frequencies to provide a confidence of the theories.
        Map<List<String>, List<TheoryCounter>> lhs2TheoryCounter = new HashMap<>();
        for (TheoryCounter tc : result) {
            lhs2TheoryCounter.computeIfAbsent(Arrays.asList(tc.getTheory().lhs), k -> new ArrayList<>()).add(tc);
        }
        List<String> lhs = new ArrayList<>();
        for (List<String> row : this.sample) {
            lhs.clear();
            for (int lhsColumnId : fd.getLhsColumnIds()) {
                int index = this.idUtils.getLocalColumnId(lhsColumnId);
                lhs.add(row.get(index));
            }
            List<TheoryCounter> theoryCounters = lhs2TheoryCounter.getOrDefault(lhs, Collections.emptyList());
            for (TheoryCounter tc : theoryCounters) {
                tc.lhsCounter++;
            }
        }

        return result;
    }

    private Theory createTheory(FunctionalDependency fd, List<String> row) {
        String[] lhs = new String[fd.getArity()];
        int i = 0;
        for (int lhsColumnId : fd.getLhsColumnIds()) {
            int lhsIndex = this.idUtils.getLocalColumnId(lhsColumnId);
            lhs[i++] = row.get(lhsIndex);
        }
        int rhsIndex = this.idUtils.getLocalColumnId(fd.getRhsColumnId());
        String rhs = row.get(rhsIndex);
        return new Theory(lhs, rhs);
    }

    /**
     * Describes a concrete set of values for a {@link FunctionalDependency}.
     */
    public static class Theory {

        private final String[] lhs;

        private final String rhs;

        private Theory(String[] lhs, String rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        public String[] getLhs() {
            return lhs;
        }

        public String getRhs() {
            return rhs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Theory theory = (Theory) o;
            return Arrays.equals(lhs, theory.lhs) &&
                    Objects.equals(rhs, theory.rhs);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(lhs), rhs);
        }
    }

    /**
     * Describes how often a {@link Theory} appears.
     */
    public static class TheoryCounter {

        private final Theory theory;

        private int counter, lhsCounter;

        public TheoryCounter(Theory theory, int counter) {
            this.theory = theory;
            this.counter = counter;
        }

        public Theory getTheory() {
            return theory;
        }

        public int getCounter() {
            return counter;
        }

        public int getLhsCounter() {
            return lhsCounter;
        }

        /**
         * Formats this instance in easily readable manner.
         *
         * @param fd    to which this instance belongs
         * @param store which stores the schema
         * @return a formatted {@link String}
         */
        public String format(FunctionalDependency fd, MetadataStore store) {
            StringBuilder sb = new StringBuilder();
            sb.append(this.counter).append("x [");
            String separator = "";
            for (int i = 0; i < fd.getArity(); i++) {
                int columnId = fd.getLhsColumnIds()[i];
                Target column = store.getTargetById(columnId);
                sb.append(separator).append(column.getName()).append("=\"").append(this.theory.lhs[i]).append("\"");
                separator = ", ";
            }
            sb.append("] \u2192 ");
            Target column = store.getTargetById(fd.getRhsColumnId());
            sb.append(column.getName()).append("=\"").append(this.theory.rhs).append("\"");
            sb.append(" (confidence=").append(String.format("%.3f", this.counter / (double) this.lhsCounter)).append(")");

            return sb.toString();
        }
    }

}
