package de.hpi.isg.pyro.model;

import de.hpi.isg.pyro.util.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a relational table using a row-based memory layout.
 */
public class RowLayoutRelation extends Relation {

    private final int[][] tuples;

    public static RowLayoutRelation createFrom(RelationalInputGenerator fileInputGenerator, boolean isNullEqualNull)
            throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        return createFrom(fileInputGenerator, isNullEqualNull, -1, -1);
    }

    public static RowLayoutRelation createFrom(RelationalInputGenerator fileInputGenerator, boolean isNullEqualNull,
                                               int maxCols, long maxRows)
            throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        long _startMillis = System.currentTimeMillis();
        try (final RelationalInput relationalInput = fileInputGenerator.generateNewCopy()) {

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

            // Create the PLIs.
            List<PositionListIndex> positionListIndices = new ArrayList<>(columnVectors.size());
            for (IntList columnVector : columnVectors) {
                positionListIndices.add(PositionListIndex.createFor(columnVector, isNullEqualNull));
            }

            // Create the tuples.
            int numTuples = columnVectors.get(0).size();
            int[][] tuples = new int[numTuples][numColumns];
            int columnIndex = 0;
            for (PositionListIndex positionListIndex : positionListIndices) {
                int clusterId = 1;
                for (IntArrayList cluster : positionListIndex.getIndex()) {
                    for (IntIterator iter = cluster.iterator(); iter.hasNext(); ) {
                        int tupleIndex = iter.nextInt();
                        tuples[tupleIndex][columnIndex] = clusterId;
                    }
                    clusterId++;
                }
                columnIndex++;
            }

            // Create the object.
            RowLayoutRelation relation = new RowLayoutRelation(relationalInput.relationName(), tuples, isNullEqualNull);
            for (int i = 0; i < numColumns; i++) {
                relation.columns.add(new Column(relation, relationalInput.columnNames().get(i), i, positionListIndices.get(i)));
            }
            relation._loadMillis = System.currentTimeMillis() - _startMillis;
            return relation;

        } catch (Exception e) {
            throw new RuntimeException("Could not build relation.", e);
        }
    }

    public RowLayoutRelation(String name, int[][] tuples, boolean isNullEqualNull) {
        super(name, isNullEqualNull);
        this.tuples = tuples;
    }

    public int getNumRows() {
        return this.tuples.length;
    }

    @Override
    public int[] getTuple(int index) {
        return this.tuples[index];
    }

    public PositionListIndex getPositionListIndex(int index) {
        return this.getColumn(index).getPositionListIndex();
    }

}
