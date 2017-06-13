package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.PartialFD;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.backend.result_receiver.ResultReader;
import de.metanome.backend.results_db.ResultType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A representation of functional dependencies as plain text.
 */
public class PlainTextFD {

    public final List<String> lhs;

    public final String rhs;

    public PlainTextFD(PartialFD partialFD) {
        this(extractLhs(partialFD), partialFD.rhs.getName());
    }

    public PlainTextFD(FunctionalDependency functionalDependency) {
        this.lhs = new ArrayList<>(functionalDependency.getDeterminant().getColumnIdentifiers().size());
        for (ColumnIdentifier columnIdentifier : functionalDependency.getDeterminant().getColumnIdentifiers()) {
            lhs.add(columnIdentifier.getColumnIdentifier());
        }

        this.rhs = functionalDependency.getDependant().getColumnIdentifier();
    }

    private static List<String> extractLhs(PartialFD partialFD) {
        List<String> lhs = new ArrayList<>(partialFD.lhs.getColumns().length);
        for (Column column : partialFD.lhs.getColumns()) {
            lhs.add(column.getName());
        }
        return lhs;
    }

    public PlainTextFD(List<String> lhs, String rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    /**
     * Load a file written with {@link FDPersistence}.
     */
    public static List<PlainTextFD> loadFromTextFile(String path) throws IOException {
        List<PlainTextFD> result = new ArrayList<>();
        try (FDPersistence.Reader reader = FDPersistence.createReader(path)) {
            PlainTextFD fd;
            while ((fd = reader.read()) != null) result.add(fd);
        }
        return result;
    }

    /**
     * Load a file written with {@link de.metanome.backend.result_receiver.ResultPrinter Metanome}.
     */
    public static List<PlainTextFD> loadFromMetanome(String path) throws IOException {
        // Load the FDs using Metanome.
        ResultReader<FunctionalDependency> reader = new ResultReader<>(ResultType.FD);
        List<FunctionalDependency> functionalDependencies = reader.readResultsFromFile(path);

        // Convert the FDs.
        List<PlainTextFD> plainTextFDs = new ArrayList<>(functionalDependencies.size());
        for (FunctionalDependency functionalDependency : functionalDependencies) {
            plainTextFDs.add(new PlainTextFD(functionalDependency));
        }

        return plainTextFDs;
    }

    @Override
    public String toString() {
        return String.format("%s -> %s", lhs, rhs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlainTextFD that = (PlainTextFD) o;
        return Objects.equals(lhs, that.lhs) &&
                Objects.equals(rhs, that.rhs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lhs, rhs);
    }
}
