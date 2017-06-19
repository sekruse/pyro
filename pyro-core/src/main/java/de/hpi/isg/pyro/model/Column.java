package de.hpi.isg.pyro.model;

import de.metanome.algorithm_integration.ColumnIdentifier;

import java.util.BitSet;
import java.util.Objects;

/**
 * Represents a column in a {@link RelationSchema} (i.e., without data).
 */
public class Column implements Vertical {

    private final RelationSchema schema;

    private final String name;

    private final int index;

    private final BitSet indices;


    Column(RelationSchema schema, String name, int index) {
        this.schema = schema;
        this.name = name;
        this.index = index;
        this.indices = new BitSet(index + 1);
        this.indices.set(index);
    }

    public int getIndex() {
        return this.index;
    }

    @Override
    public BitSet getColumnIndices() {
        return this.indices;
    }

    public String getName() {
        return this.name;
    }

    public RelationSchema getSchema() {
        return this.schema;
    }

    @Override
    public String toString() {
        return '[' + this.name + ']';
    }

    /**
     * Convert this instance into a {@link ColumnIdentifier}.
     *
     * @return the converted instance
     */
    public ColumnIdentifier toMetanomeColumnIdentifier() {
        return new ColumnIdentifier(this.getSchema().getName(), this.getName());
    }

    public Column copyFor(RelationSchema relationCopy) {
        return new Column(
                relationCopy,
                this.name,
                this.index
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return index == column.index &&
                Objects.equals(schema, column.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, index);
    }
}
