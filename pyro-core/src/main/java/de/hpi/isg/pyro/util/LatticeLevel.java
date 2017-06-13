package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.ColumnCombination;

import java.util.*;

/**
 * Describes a level in the powerset lattice consisting of {@link LatticeVertex LatticeVertices}.
 */
public class LatticeLevel {

    /**
     * The arity of the  {@link LatticeVertex LatticeVertices}.
     */
    private final int arity;

    private final Map<BitSet, LatticeVertex> vertices = new HashMap<>();

    public LatticeLevel(int arity) {
        this.arity = arity;
    }

    public int getArity() {
        return arity;
    }

    public Map<BitSet, LatticeVertex> getVertices() {
        return vertices;
    }

    public void add(LatticeVertex vertex) {
        this.vertices.put(vertex.getVertical().getColumnIndices(), vertex);
    }

    public LatticeVertex getLatticeVertex(BitSet columnIndices) {
        return this.vertices.get(columnIndices);
    }

    /**
     * Generates a new {@link LatticeLevel} on top of the existing ones.
     *
     * @param levels the {@link LatticeLevel}s starting from arity 0
     */
    public static void generateNextLevel(List<LatticeLevel> levels) {
        int arity = levels.size() - 1;
        System.out.printf("Creating level %d...\n", arity + 1);
        LatticeLevel currentLevel = levels.get(arity);
        ArrayList<LatticeVertex> currentLevelVertices = new ArrayList<>(currentLevel.getVertices().values());
        currentLevelVertices.sort(LatticeVertex::compareTo);
        LatticeLevel nextLevel = new LatticeLevel(arity + 1);
        for (int vertexIndex1 = 0; vertexIndex1 < currentLevelVertices.size(); vertexIndex1++) {
            LatticeVertex vertex1 = currentLevelVertices.get(vertexIndex1);
            if (vertex1.getRhsCandidates().isEmpty() && !vertex1.isKeyCandidate()) continue;

            Vertex2:
            for (int vertexIndex2 = vertexIndex1 + 1; vertexIndex2 < currentLevelVertices.size(); vertexIndex2++) {
                LatticeVertex vertex2 = currentLevelVertices.get(vertexIndex2);

                // Check whether vertex1 comes before vertex2 and have the same prefix.
                if (!vertex1.comesBeforeAndSharePrefixWith(vertex2)) {
                    break;
                }

                // Quick check: Will there be any RHS candidates?
                if (!vertex1.getRhsCandidates().intersects(vertex1.getRhsCandidates()) && !vertex2.isKeyCandidate()) continue;

                final ColumnCombination childColumns = (ColumnCombination) vertex1.getVertical().union(vertex2.getVertical());
                LatticeVertex childVertex = new LatticeVertex(childColumns);

                // We have <c1, ..., ck-1, ck> and <c1, ..., ck-1, ck+1>., now generate <c2, ..., ck, ck+1> ...
                // and retrieve the corresponding vertices.
                BitSet parentIndices = new BitSet();
                parentIndices.or(vertex1.getVertical().getColumnIndices());
                parentIndices.or(vertex2.getVertical().getColumnIndices());

                childVertex.getRhsCandidates().or(vertex1.getRhsCandidates());
                childVertex.getRhsCandidates().and(vertex2.getRhsCandidates());
                childVertex.setKeyCandidate(vertex1.isKeyCandidate() && vertex2.isKeyCandidate());
                childVertex.setInvalid(vertex1.isInvalid() || vertex2.isInvalid());

                for (int i = 0, skipIndex = parentIndices.nextSetBit(0);
                     i < arity - 1;
                     i++, skipIndex = parentIndices.nextSetBit(skipIndex + 1)) {
                    parentIndices.clear(skipIndex);
                    final LatticeVertex parentVertex = currentLevel.getLatticeVertex(parentIndices);
                    if (parentVertex == null) continue Vertex2;
                    childVertex.getRhsCandidates().and(parentVertex.getRhsCandidates());
                    if (childVertex.getRhsCandidates().isEmpty()) continue Vertex2;
                    childVertex.getParents().add(parentVertex);
                    parentIndices.set(skipIndex);

                    childVertex.setKeyCandidate(childVertex.isKeyCandidate() && parentVertex.isKeyCandidate());
                    childVertex.setInvalid(childVertex.isInvalid() || parentVertex.isInvalid());

                    if (!childVertex.isKeyCandidate() && childVertex.getRhsCandidates().isEmpty()) {
                        continue Vertex2;
                    }
                }

                childVertex.getParents().add(vertex1);
                childVertex.getParents().add(vertex2);

                nextLevel.add(childVertex);
            }
        }
        levels.add(nextLevel);

    }

    /**
     * Clear all {@link LatticeLevel}s below the given {@code arity}.
     *
     * @param levels the {@link LatticeLevel}s indexed by their arity
     * @param arity  the arity
     */
    public static void clearLevelsBelow(List<LatticeLevel> levels, int arity) {
        // Clear the levels from the level list.
        for (int i = 0; i < Math.min(levels.size(), arity ); i++) {
            levels.get(i).getVertices().clear();
        }

        // Clear child references.
        if (arity < levels.size()) {
            for (LatticeVertex retainedVertex : levels.get(arity).getVertices().values()) {
                retainedVertex.getParents().clear();
            }
        }
    }

}
