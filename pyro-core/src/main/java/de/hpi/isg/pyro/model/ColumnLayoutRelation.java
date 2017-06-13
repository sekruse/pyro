package de.hpi.isg.pyro.model;

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
public class ColumnLayoutRelation extends Relation {

    public static ColumnLayoutRelation createFrom(RelationalInputGenerator fileInputGenerator, boolean isNullEqualNull)
            throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
        return createFrom(fileInputGenerator, isNullEqualNull, -1, -1);
    }

    public static ColumnLayoutRelation createFrom(RelationalInputGenerator fileInputGenerator, boolean isNullEqualNull,
                                                  int maxCols, long maxRows)
            throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {

        long _startMillis = System.currentTimeMillis();
        try (final RelationalInput relationalInput = fileInputGenerator.generateNewCopy()) {

            // Prepare the relation.
            final ColumnLayoutRelation relation = new ColumnLayoutRelation(relationalInput.relationName(), isNullEqualNull);

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
            int index = 0;
            for (IntList columnVector : columnVectors) {
                PositionListIndex pli = PositionListIndex.createFor(columnVector, relation.isNullEqualNull());
                relation.columns.add(new Column(relation, relationalInput.columnNames().get(index), index, pli));
                index++;
            }

            relation._loadMillis = System.currentTimeMillis() - _startMillis;
            return relation;

        } catch (Exception e) {
            throw new RuntimeException("Could not build relation vectors.", e);
        }
    }

    private ColumnLayoutRelation(String name, boolean isNullEqualNull) {
        super(name, isNullEqualNull);
    }

    public String getName() {
        return this.name;
    }

    public int getNumRows() {
        return this.columns.get(0).getData().length;
    }

    @Override
    public int[] getTuple(int tupleIndex) {
        int[] tuple = new int[this.getNumColumns()];
        for (int columnIndex = 0; columnIndex < this.getNumColumns(); columnIndex++) {
            tuple[columnIndex] = this.columns.get(columnIndex).getData()[tupleIndex];
        }
        return tuple;
    }

    @Override
    public void shuffleColumns() {
        for (Column column : this.columns) {
            column.shuffle();
        }
    }

    @Override
    public ColumnLayoutRelation copy() {
        ColumnLayoutRelation copy = new ColumnLayoutRelation(this.name, this.isNullEqualNull());
        for (Column column : this.columns) {
            copy.columns.add(column.copyFor(copy));
        }
        return copy;
    }
}
