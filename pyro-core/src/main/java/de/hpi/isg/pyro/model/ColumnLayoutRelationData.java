package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.Parallel;
import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a relational table.
 */
public class ColumnLayoutRelationData extends RelationData {

    /**
     * {@link ColumnData} objects aligned with the {@link Column}s in the {@link #schema}.
     */
    private final ColumnData[] columnData;

    public static ColumnLayoutRelationData createFrom(RelationalInputGenerator fileInputGenerator,
                                                      boolean isNullEqualNull)
            throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        return createFrom(fileInputGenerator, isNullEqualNull, -1, -1, Parallel.threadLocalExecutor);
    }

    public static ColumnLayoutRelationData createFrom(RelationalInputGenerator fileInputGenerator,
                                                      boolean isNullEqualNull,
                                                      int maxCols,
                                                      long maxRows,
                                                      Parallel.Executor executor)
            throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {

        long _startMillis = System.currentTimeMillis();
        try (final RelationalInput relationalInput = fileInputGenerator.generateNewCopy()) {

            // Create the schema.
            final RelationSchema schema = new RelationSchema(relationalInput.relationName(), isNullEqualNull);

            // Prepare the value dictionary.
            Object2IntMap<String> valueDictionary = new Object2IntOpenHashMap<>();

            final int unknownValueId = 0;
            int nextValueId = 1;
            valueDictionary.defaultReturnValue(unknownValueId);

            // Prepare the column vectors.
            int numColumns = relationalInput.numberOfColumns();
            if (maxCols > 0) numColumns = Math.min(numColumns, maxCols);
            List<IntList> columnVectors = new ArrayList<>(numColumns);
            for (int index = 0; index < numColumns; index++) {
                columnVectors.add(new IntArrayList());
            }

            // Iterate the data and fill the column vectors.
            long rowNum = 0L;
            while (relationalInput.hasNext()) {
                final List<String> row = relationalInput.next();
                int index = 0;
                for (String field : row) {
                    if (field == null) {
                        columnVectors.get(index).add(nullValueId);
                    } else {
                        int valueId = valueDictionary.getInt(field);
                        if (valueId == unknownValueId) {
                            valueDictionary.put(field, valueId = nextValueId++);
                        }
                        columnVectors.get(index).add(valueId);
                    }
                    if (++index >= numColumns) break;
                }
                if (maxRows > 0 && ++rowNum >= maxRows) break;
            }
            valueDictionary = null;

            // Create the actual data structures.
            List<ColumnData> columnDataList = Parallel.map(
                    Parallel.zipWithIndex(columnVectors),
                    columnVectorWithIndex -> {
                        // Declare the column.
                        int index = columnVectorWithIndex.f2;
                        Column column = new Column(schema, relationalInput.columnNames().get(index), index);

                        // Create the column data.
                        IntList columnVector = columnVectorWithIndex.f1;
                        PositionListIndex pli = PositionListIndex.createFor(columnVector, schema.isNullEqualNull());
                        return new ColumnData(column, pli.getProbingTable(true), pli);
                    },
                    executor,
                    true
            );
            final ColumnData[] columnData = new ColumnData[columnVectors.size()];
            for (ColumnData data : columnDataList) {
                schema.columns.add(data.getColumn());
                columnData[data.getColumn().getIndex()] = data;
            }
            ColumnLayoutRelationData instance = new ColumnLayoutRelationData(schema, columnData);

//            relation._loadMillis = System.currentTimeMillis() - _startMillis;
            return instance;

        } catch (Exception e) {
            throw new RuntimeException("Could not build relation vectors.", e);
        }
    }

    /**
     * Retrieves the {@link ColumnData} for the {@link Column}s in the {@link #getSchema() schema} - in the exact same order.
     *
     * @return an array of {@link ColumnData}
     */
    public ColumnData[] getColumnData() {
        return this.columnData;
    }

    /**
     * Retrieves the {@link ColumnData} for the {@link Column} with the given index.
     *
     * @param columnIndex the index of the {@link Column}
     * @return the {@link ColumnData}
     */
    public ColumnData getColumnData(int columnIndex) {
        return this.columnData[columnIndex];
    }

    private ColumnLayoutRelationData(RelationSchema schema, ColumnData[] columnData) {
        super(schema);
        this.columnData = columnData;
    }

    public int getNumRows() {
        return this.columnData[0].getProbingTable().length;
    }

    @Override
    public int[] getTuple(int tupleIndex) {
        int numColumns = this.getSchema().getNumColumns();
        int[] tuple = new int[numColumns];
        for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
            tuple[columnIndex] = this.columnData[columnIndex].getProbingTable()[tupleIndex];
        }
        return tuple;
    }

    @Override
    public void shuffleColumns() {
        for (ColumnData columnDatum : this.columnData) {
            columnDatum.shuffle();
        }
    }
}
