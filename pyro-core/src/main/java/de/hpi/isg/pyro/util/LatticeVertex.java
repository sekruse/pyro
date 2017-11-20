package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Vertical;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;

/**
 * Describes a vertix in the powerset lattice of some {@link Column}s for TANE-style algorithms.
 */
public class LatticeVertex implements Comparable<LatticeVertex> {

    private final Vertical vertical;

    private PositionListIndex positionListIndex;

    private final BitSet rhsCandidates = new BitSet();

    private boolean isKeyCandidate = false;

    private final List<LatticeVertex> parents = new ArrayList<>();

    /**
     * Invalid nodes are proposed in the "Functional Dependency Discovery: An Experimental Evaluation of Seven Algorithms"
     * (Papenbrock et al., 2015). They fix an issue with Tane's key pruning rule by retaining FD candidates but not
     * creating {@link PositionListIndex}es anymore.
     */
    private boolean isInvalid = false;

    public LatticeVertex(Vertical vertical) {
        this.vertical = vertical;
    }

    public Vertical getVertical() {
        return vertical;
    }

    public BitSet getRhsCandidates() {
        return rhsCandidates;
    }

    public List<LatticeVertex> getParents() {
        return parents;
    }

    public void addRhsCandidates(Collection<? extends Vertical> candidates) {
        for (Vertical candidate : candidates) {
            BitSets.setAll(this.rhsCandidates, candidate.getColumnIndices());
        }
    }

    public BitSet getBlockingPrefix() {
        final BitSet prefix = new BitSet();
        prefix.or(this.vertical.getColumnIndices());
        if (prefix.isEmpty()) throw new IllegalStateException();
        int lastBitPos = prefix.nextSetBit(0);
        while (prefix.nextSetBit(lastBitPos + 1) != -1) {
            lastBitPos = prefix.nextSetBit(lastBitPos + 1);
        }
        prefix.clear(lastBitPos);
        return prefix;
    }

    public boolean comesBeforeAndSharePrefixWith(LatticeVertex that) {
        final BitSet thisIndices = this.getVertical().getColumnIndices();
        final BitSet thatIndices = that.getVertical().getColumnIndices();

        int thisIndex = thisIndices.nextSetBit(0);
        int thatIndex = thatIndices.nextSetBit(0);

        int arity = thisIndices.cardinality();
        for (int i = 0; i < arity - 1; i++) {
            if (thisIndex != thatIndex) return false;
            thisIndex = thisIndices.nextSetBit(thisIndex + 1);
            thatIndex = thatIndices.nextSetBit(thatIndex + 1);
        }

        return thisIndex < thatIndex;
    }

    public void setKeyCandidate(boolean isKeyCandidate) {
        this.isKeyCandidate = isKeyCandidate;
    }

    public boolean isKeyCandidate() {
        return this.isKeyCandidate;
    }

    public boolean isInvalid() {
        return isInvalid;
    }

    public void setInvalid(boolean invalid) {
        isInvalid = invalid;
    }

    public PositionListIndex getPositionListIndex() {
        return this.positionListIndex;
    }

    public void setPositionListIndex(PositionListIndex positionListIndex) {
        this.positionListIndex = positionListIndex;
    }

    @Override
    public int compareTo(LatticeVertex that) {
        // First order by arity.
        int result = Integer.compare(this.vertical.getArity(), that.vertical.getArity());
        if (result != 0) return result;

        // Otherwise, compare the single indices lexicographically.
        BitSet thisIndices = this.vertical.getColumnIndices();
        int thisIndex = thisIndices.nextSetBit(0);
        BitSet thatIndices = that.vertical.getColumnIndices();
        int thatIndex = thatIndices.nextSetBit(0);

        // Move forward until we find equal column indices (which could also be -1).
        while (true) {
            result = Integer.compare(thisIndex, thatIndex);
            if (result != 0) return result;
            thisIndex = thisIndices.nextSetBit(thisIndex + 1);
            thatIndex = thatIndices.nextSetBit(thatIndex + 1);
        }
    }

    @Override
    public String toString() {
        return String.format("Vtx%s", this.vertical);
    }
}
