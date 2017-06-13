package de.hpi.isg.pyro.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.BitSet;

/**
 * Utility to operate on {@link java.util.BitSet}s.
 */
public class BitSets {

    public static void setAll(BitSet accumulator, BitSet delta) {
        for (int bitPos = delta.nextSetBit(0); bitPos != -1; bitPos = delta.nextSetBit(bitPos + 1)) {
            accumulator.set(bitPos);
        }
    }

    public static boolean isSubset(BitSet subset, BitSet superset) {
        if (subset.cardinality() > superset.cardinality()) return false;
        for (int i = subset.nextSetBit(0); i != -1; i = subset.nextSetBit(i + 1)) {
            if (!superset.get(i)) return false;
        }
        return true;
    }

    public static IntArrayList toIntArrayList(BitSet bitSet) {
        IntArrayList intArrayList = new IntArrayList(bitSet.cardinality());
        for (int i = bitSet.nextSetBit(0); i != -1; i = bitSet.nextSetBit(i + 1)) {
            intArrayList.add(i);
        }
        return intArrayList;
    }

    public static int[] toIntArray(BitSet bitSet) {
        int[] array = new int[bitSet.cardinality()];
        for (int index = bitSet.nextSetBit(0), i = 0;
             index != -1;
             index = bitSet.nextSetBit(index + 1), i++) {
            array[i] = index;
        }
        return array;
    }

    public static int intersectionSize(BitSet a, BitSet b) {
        int size = 0;
        int nextA = a.nextSetBit(0);
        int nextB = b.nextSetBit(0);
        while (nextA != -1 && nextB != -1) {
            if (nextA == nextB) {
                size++;
                nextA = a.nextSetBit(nextA + 1);
                nextB = b.nextSetBit(nextB + 1);
            } else if (nextA < nextB) {
                nextA = a.nextSetBit(nextA + 1);
            } else if (nextB < nextA) {
                nextB = b.nextSetBit(nextB + 1);
            }
        }
        return size;
    }

}
