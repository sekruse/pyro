package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.RelationData;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleBinaryOperator;

/**
 * Also called <i>stripped partition</i>. Maps value IDs to the positions in a {@link Column} (or combinations thereof).
 * Value appearing only once are omitted, though.
 */
public class PositionListIndex {

    private final ArrayList<IntArrayList> index;

    /**
     * If explicitly requested, the probing table can be cached.
     */
    private int[] probingTableCache = null;

    /**
     * If there is any {@code null}s in the indexed vertical, then this member contains exactly that cluster. It
     * is also present in the {@link #index}.
     */
    private final IntArrayList nullCluster;

    private final double entropy;

    private final long nep;

    private final int relationSize, originalRelationSize, size;

    public static final int singletonValueId = 0;

    private static final int nullValueId = RelationData.nullValueId;

    // Profiling data.
    public static final AtomicLong _numIntersects = new AtomicLong(0);
    public static final AtomicLong _nanosIntersects = new AtomicLong(0);

    public static PositionListIndex createFor(int[] data, boolean isNullEqualNull) {
        // Create the raw index by iterating over the column vector.
        Int2ObjectMap<IntArrayList> index = new Int2ObjectOpenHashMap<>();
        for (int position = 0; position < data.length; position++) {
            int valueId = data[position];
            IntArrayList positions = index.get(valueId);
            if (positions == null) {
                index.put(valueId, positions = new IntArrayList());
            }
            positions.add(position);
        }

        // Post-process the index.
        IntArrayList nullCluster = isNullEqualNull ?
                index.get(RelationData.nullValueId) :
                index.remove(RelationData.nullValueId);

        double keyGap = 0d;
        long nep = 0;
        int size = 0;
        for (Iterator<Int2ObjectMap.Entry<IntArrayList>> iterator = index.int2ObjectEntrySet().iterator();
             iterator.hasNext(); ) {

            final Int2ObjectMap.Entry<IntArrayList> entry = iterator.next();
            final IntArrayList positions = entry.getValue();
            if (positions.size() == 1) {
                iterator.remove();
                continue;
            }

            positions.trim();
            keyGap += positions.size() * Math.log(positions.size());
            nep += calculateNep(positions.size());
            size += positions.size();
        }

        double entropy = Math.log(data.length) - keyGap / data.length;

        ArrayList<IntArrayList> clusters = new ArrayList<>(index.size());
        clusters.addAll(index.values());
        sortClusters(clusters);

        return new PositionListIndex(clusters, nullCluster, size, entropy, nep, data.length, data.length);
    }

    public static PositionListIndex createFor(IntList data, boolean isNullEqualNull) {
        // Create the raw index by iterating over the column vector.
        Int2ObjectMap<IntArrayList> index = new Int2ObjectOpenHashMap<>();
        int position = 0;
        for (IntListIterator iterator = data.iterator(); iterator.hasNext(); position++) {
            int valueId = iterator.nextInt();
            IntArrayList positions = index.get(valueId);
            if (positions == null) {
                index.put(valueId, positions = new IntArrayList());
            }
            positions.add(position);
        }

        // Post-process the index.
        IntArrayList nullCluster = isNullEqualNull ?
                index.get(RelationData.nullValueId) :
                index.remove(RelationData.nullValueId);
        double keyGap = 0d;
        long nep = 0;
        int size = 0;
        for (Iterator<Int2ObjectMap.Entry<IntArrayList>> iterator = index.int2ObjectEntrySet().iterator();
             iterator.hasNext(); ) {

            final Int2ObjectMap.Entry<IntArrayList> entry = iterator.next();
            final IntArrayList positions = entry.getValue();
            if (positions.size() == 1) {
                iterator.remove();
                continue;
            }

            positions.trim();
            keyGap += positions.size() * Math.log(positions.size());
            nep += calculateNep(positions.size());
            size += positions.size();
        }

        double entropy = Math.log(data.size()) - keyGap / data.size();

        ArrayList<IntArrayList> clusters = new ArrayList<>(index.size());
        clusters.addAll(index.values());
        sortClusters(clusters);

        return new PositionListIndex(clusters, nullCluster, size, entropy, nep, data.size(), data.size());
    }

    /**
     * Sort clusters by their first element.
     *
     * @param clusters the data structure to sort
     */
    private static void sortClusters(ArrayList<IntArrayList> clusters) {
        clusters.sort(Comparator.comparing(cluster -> cluster.getInt(0)));
    }

    private static long calculateNep(long numElements) {
        return numElements * (numElements - 1) / 2;
    }

    private PositionListIndex(ArrayList<IntArrayList> index,
                              IntArrayList nullCluster,
                              int size,
                              double entropy,
                              long nep,
                              int relationSize,
                              int originalRelationSize) {
        this.index = index;
        this.nullCluster = nullCluster;
        this.size = size;
        this.entropy = entropy;
        this.relationSize = relationSize;
        this.originalRelationSize = originalRelationSize;
        this.nep = nep;
    }

    public PositionListIndex intersect(PositionListIndex that) {
        assert this.relationSize == that.relationSize;

        // Probing the smaller PLI is faster.
        long _startNanos = System.nanoTime();
        PositionListIndex result = this.size > that.size ?
            that.probe(this.getProbingTable()) :
            this.probe(that.getProbingTable());
        _nanosIntersects.addAndGet(System.nanoTime() - _startNanos);
        _numIntersects.incrementAndGet();
        return result;
    }

    /**
     *
     * NB: Null clusters are not supported.
     * @param probingTable
     * @return
     */
    public PositionListIndex probe(int[] probingTable) {
        assert this.relationSize == probingTable.length;

        // Prepare the new index.
        ArrayList<IntArrayList> newIndex = new ArrayList<>();
        int newSize = 0;
        double newKeyGap = 0d;
        long newNep = 0;

        // Prepare a partial index to do the probing on each cluster.
        Int2ObjectMap<IntArrayList> partialIndex = new Int2ObjectOpenHashMap<>();
        IntArrayList nullCluster = null;

        // Do the actual probing cluster by cluster.
        for (IntArrayList positions : this.index) {
            for (IntIterator iterator = positions.iterator(); iterator.hasNext(); ) {
                // Probe the position.
                final int position = iterator.nextInt();
                final int probingTableValueId = probingTable[position];

                // Skip singleton values.
                if (probingTableValueId == singletonValueId) continue;

                // Insert the probed position into the partial index.
                IntArrayList newCluster = partialIndex.get(probingTableValueId);
                if (newCluster == null) {
                    partialIndex.put(probingTableValueId, newCluster = new IntArrayList(4));
                    // Note that we explicitly use small initial cluster sizes, as (i) we otherwise might run into
                    // memory problems, and (ii) the amortized complexity of enlarging arrays is O(n).
                    if (probingTableValueId == nullValueId && positions == this.nullCluster) {
                        nullCluster = newCluster;
                    }
                }
                newCluster.add(position);
            }

            // Copy the partial index into the new PLI.
            for (IntArrayList cluster : partialIndex.values()) {
                // Skip singleton clusters.
                if (cluster.size() == 1) continue;

                cluster.trim();
                newSize += cluster.size();
                newKeyGap += cluster.size() * Math.log(cluster.size());
                newNep += calculateNep(cluster.size());
                newIndex.add(cluster);
            }
            partialIndex.clear();
        }

        double newEntropy = Math.log(this.relationSize) - newKeyGap / this.relationSize;

        sortClusters(newIndex);
        newIndex.trimToSize();

        return new PositionListIndex(newIndex, nullCluster, newSize, newEntropy, newNep, this.relationSize, this.relationSize);
    }

    /**
     * Retrieve a probing table for this instance. Note that it might be necessary to construct the probing table first.
     *
     * @return the probing table
     */
    public int[] getProbingTable() {
        return this.getProbingTable(false);
    }

    /**
     * Retrieve a probing table for this instance. Note that it might be necessary to construct the probing table first.
     *
     * @param isCaching the probing table should be cached if not existent yet
     * @return the probing table
     */
    public int[] getProbingTable(boolean isCaching) {
        if (this.probingTableCache != null) return this.probingTableCache;

        int[] probingTable = new int[this.originalRelationSize];
        int nextClusterId = singletonValueId + 1;
        for (IntArrayList cluster : this.index) {
            final int valueId = nextClusterId++;
            assert valueId != singletonValueId : String.format("Singleton value ID %d is not allowed.", singletonValueId);
            for (IntIterator iterator = cluster.iterator(); iterator.hasNext(); ) {
                int position = iterator.next();
                probingTable[position] = valueId;
            }
        }

        if (isCaching) this.probingTableCache = probingTable;
        return probingTable;
    }

    public int getClusterSize(int probingTableId) {
        switch (probingTableId) {
            case singletonValueId:
                return 1;
            case nullValueId:
                return this.nullCluster == null ? 0 : this.nullCluster.size();
            default:
                return this.index.get(probingTableId - 1).size();
        }
    }

    public Volatility calculateVolatility(PositionListIndex that, boolean isRegardRedundantEPs) {
        int[] probingTable = that.getProbingTable();
        assert this.relationSize == probingTable.length;

        // Prepare a partial index to do the probing on each cluster.
        Int2IntOpenHashMap intersectionSizes = new Int2IntOpenHashMap();
        long totalGain = 0L;
        int numGainTuples = 0, numLossTuples = 0, numDrawTuples = 0;
        long numEffectiveTuplePairs = this.relationSize * (this.relationSize - 1L) / 2;
        long numCommonEPs = 0;

        // Calculate the cluster intersections.
        for (IntArrayList thisCluster : this.index) {
            for (IntIterator iterator = thisCluster.iterator(); iterator.hasNext(); ) {
                // Probe the position.
                final int position = iterator.nextInt();
                final int probingTableValueId = probingTable[position];

                // Skip singleton values.
                if (probingTableValueId == singletonValueId) continue;

                // Insert the probed position into the partial index.
                intersectionSizes.addTo(probingTableValueId, 1);
            }

            // Evaluate the cluster intersections.
            ObjectIterator<Int2IntMap.Entry> intersectionSizeIterator = intersectionSizes.int2IntEntrySet().fastIterator();
            while (intersectionSizeIterator.hasNext()) {
                Int2IntMap.Entry entry = intersectionSizeIterator.next();
                int thatClusterId = entry.getIntKey();
                int intersectionSize = entry.getIntValue();
                int thatClusterSize = that.getClusterSize(thatClusterId);

                // (i) Reason on the possible gain:
                // We count the tuple pairs, such that one tuple has the same "x" value and the other has the same
                // "y" value as this cluster. The number of this tuple pairs is exactly the number of possibilities
                // that we could create a new tuple for the current cluster.
                long numOtherX = thisCluster.size() - intersectionSize;
                long numOtherY = thatClusterSize - intersectionSize;
                long gain = isRegardRedundantEPs ?
                        numOtherX * numOtherY * intersectionSize :
                        numOtherX * numOtherY;
                numEffectiveTuplePairs -= intersectionSize * (intersectionSize - 1L) / 2;


                // (ii) Reason on possible loss:
                // This cluster shrinks if we exchange a value from any of its tuples with any tuples that does
                // neither have "x" nor the "y" value of this cluster.
                long numOtherXAndY = this.relationSize - numOtherX - numOtherY - intersectionSize;
                long loss = isRegardRedundantEPs ?
                        intersectionSize * numOtherXAndY * (intersectionSize - 1) :
                        (intersectionSize > 1 ? intersectionSize * numOtherXAndY : 0L);

                // Collect.
                totalGain += gain - loss;
                if (gain > loss) {
                    numGainTuples += intersectionSize;
                } else if (gain < loss) {
                    numLossTuples += intersectionSize;
                } else if (gain != 0) {
                    numDrawTuples += intersectionSize;
                }
                numCommonEPs += isRegardRedundantEPs ?
                        intersectionSize * (intersectionSize - 1L) / 2 :
                        intersectionSize - 1;
            }

            intersectionSizes.clear();
        }

        double expectedGain = totalGain / this.getMaximumNip();
        double expectedConditionalGain = numEffectiveTuplePairs == 0 ? 0 : totalGain / (double) numEffectiveTuplePairs;
        return new Volatility(expectedGain, expectedConditionalGain, numCommonEPs, numGainTuples, numLossTuples, numDrawTuples);
    }

    public PositionListIndex probeAll(Vertical probingColumns, RelationData relationData) {
        assert this.relationSize == relationData.getNumRows();

        // Prepare the new index.
        ArrayList<IntArrayList> newIndex = new ArrayList<>();
        int newSize = 0;
        double newKeyGap = 0d;
        long newNep = 0;

        // Prepare a partial index to do the probing on each cluster.
        Object2ObjectMap<IntArrayList, IntArrayList> partialIndex = new Object2ObjectOpenHashMap<>();
        // TODO: Support null cluster.
        final IntArrayList nullCluster = null;
        IntArrayList probe = new IntArrayList(probingColumns.getArity());

        // Do the actual probing cluster by cluster.
        for (IntArrayList cluster : this.index) {
            for (IntIterator iterator = cluster.iterator(); iterator.hasNext(); ) {
                // Probe the position.
                final int position = iterator.nextInt();
                if (!takeProbe(position, relationData, probingColumns, probe)) {
                    // Skip singleton values.
                    probe.clear();
                    continue;
                }

                // Insert the probed position into the partial index.
                IntArrayList newCluster = partialIndex.get(probe);
                if (newCluster == null) {
                    partialIndex.put(probe, newCluster = new IntArrayList(4));
                    // Note that we explicitly use small initial cluster sizes, as (i) we otherwise might run into
                    // memory problems, and (ii) the amortized complexity of enlarging arrays is O(n).
                    probe = new IntArrayList(probe.size());
                } else {
                    probe.clear();
                }
                newCluster.add(position);
            }

            // Copy the partial index into the new PLI.
            for (IntArrayList newCluster : partialIndex.values()) {
                // Skip singleton clusters.
                if (newCluster.size() == 1) continue;

                newCluster.trim();
                newSize += newCluster.size();
                newKeyGap += newCluster.size() * Math.log(newCluster.size());
                newNep += calculateNep(newCluster.size());
                newIndex.add(newCluster);
            }
            partialIndex.clear();
        }

        double newEntropy = Math.log(this.relationSize) - newKeyGap / this.relationSize;

        sortClusters(newIndex);
        newIndex.trimToSize();

        return new PositionListIndex(newIndex, nullCluster, newSize, newEntropy, newNep, this.relationSize, this.relationSize);

    }

    private boolean takeProbe(int position, RelationData relationData, Vertical probingColumns, IntArrayList probe) {
        final BitSet probingIndices = probingColumns.getColumnIndices();
        for (int index = probingIndices.nextSetBit(0);
             index != -1;
             index = probingIndices.nextSetBit(index + 1)) {
            final int value = relationData.getColumnData(index).getProbingTableValue(position);
            if (value == singletonValueId) return false;
            probe.add(value);
        }
        return true;
    }

    public class Volatility {

        private final int numGainTuples, numLossTuples, numDrawTuples;

        private final long numCommonEP;

        private final double expectedGain, expectedConditionalGain;

        public Volatility(double expectedGain, double expectedConditionalGain, long numCommonEP,
                          int numGainTuples, int numLossTuples, int numDrawTuples) {
            this.expectedGain = expectedGain;
            this.expectedConditionalGain = expectedConditionalGain;
            this.numCommonEP = numCommonEP;
            this.numGainTuples = numGainTuples;
            this.numLossTuples = numLossTuples;
            this.numDrawTuples = numDrawTuples;
        }

        public double getChangeVectorGainVectorSimilarity() {
            return cosineSimilarity(1, 0, numGainTuples, numLossTuples);
        }

        public double getChangeVectorLength() {
            return euclidianDistance(0, 0, numGainTuples, numLossTuples);
        }

        public double getNormalizedChangeVectorLength() {
            return euclidianDistance(0, 0, numGainTuples, numLossTuples) / relationSize;
        }

        public double getChangeVectorScore() {
            // Transpose similarity to [-1; 1].
            double similarity = this.getChangeVectorGainVectorSimilarity();
            similarity = (similarity - .5) * 2;

            // Stretch by change vector length;
            return similarity * this.getNormalizedChangeVectorLength();
        }

        public int getNumGainTuples() {
            return numGainTuples;
        }

        public int getNumLossTuples() {
            return numLossTuples;
        }

        public double getExpectedGain() {
            return expectedGain;
        }

        public double getExpectedConditionalGain() {
            return expectedConditionalGain;
        }

        public long getNumCommonEP() {
            return numCommonEP;
        }

        private final double dampedDiff(double a, double b) {
            double sign = Math.signum(a - b);
            double absolutedDiff = Math.sqrt(a) - Math.sqrt(b);
            absolutedDiff *= absolutedDiff;
            return sign * absolutedDiff;
        }

        private final double euclidianDistance(double x1, double y1, double x2, double y2) {
            double dX = x1 - x2;
            double dY = y1 - y2;
            return Math.sqrt(dX * dX + dY * dY);
        }

        private final double cosineSimilarity(double x1, double y1, double x2, double y2) {
            double dotProduct = x1 * x2 + y1 * y2;
            double lengthProduct = Math.sqrt(x1 * x1 + y1 * y1) * Math.sqrt(x2 * x2 + y2 * y2);
            return dotProduct / lengthProduct;
        }

        @Override
        public String toString() {
            return String.format("Volatility[E[gain]=%+,.3f, E[gain|change]=%+,.3f, |gain|=%,d, |loss|=%,d]",
                    this.expectedGain, this.expectedConditionalGain, this.numGainTuples, numLossTuples
            );
        }
    }

    public ArrayList<IntArrayList> getIndex() {
        return this.index;
    }

    public double getEntropy() {
        return this.entropy;
    }

    public double getMaximumNip() {
        return calculateNep(this.relationSize);
    }

    public double getNip() {
        return this.getMaximumNip() - this.getNep();
    }

    public double getNep() {
        return this.nep;
    }

    public long getNepAsLong() {
        return this.nep;
    }

    public double getNumNonRedundantEP() {
        return this.size() - this.getNumNonSingletonClusters();
    }

    /**
     * Retrieve the number of stored indices across all clusters.
     *
     * @return the number of stored indices
     */
    public int size() {
        return this.size;
    }

    public int getNumClusters() {
        return this.getNumNonSingletonClusters() + this.getNumSingletonClusters();
    }

    public int getNumNonSingletonClusters() {
        return this.index.size();
    }

    public int getNumSingletonClusters() {
        return this.relationSize - this.size;
    }

    public int getRelationSize() {
        return relationSize;
    }

    public IntArrayList getLargestCluster() {
        // Identify the largest cluster.
        IntArrayList largestCluster = null;

        for (IntArrayList cluster : this.index) {
            if (largestCluster == null || cluster.size() > largestCluster.size()) {
                largestCluster = cluster;
            }
        }

        return largestCluster == null ? new IntArrayList(0) : largestCluster;
    }

    /**
     * Create a new, stripped instance.
     *
     * @param indices that indices to be stripped
     * @return the new, stripped instance
     */
    public PositionListIndex strip(IntList indices) {
        double newKeyGap = 0d;
        long newNep = 0L;
        int newSize = 0;
        ArrayList<IntArrayList> newIndex = new ArrayList<>(this.index.size());
        IntArrayList strippedNullCluster = null;
        for (IntArrayList cluster : this.index) {
            if (isDisjoint(cluster, indices)) {
                newIndex.add(cluster);
                newNep += cluster.size() * (cluster.size() - 1) / 2;
                newKeyGap += cluster.size() * Math.log(cluster.size());
                newSize += cluster.size();
                if (cluster == this.nullCluster) {
                    strippedNullCluster = cluster;
                }
            } else {
                IntArrayList newCluster = new IntArrayList(cluster);
                newCluster.removeAll(indices);
                if (newCluster.size() > 1) {
                    newIndex.add(newCluster);
                    newNep += newCluster.size() * (newCluster.size() - 1) / 2;
                    newKeyGap += newCluster.size() * Math.log(newCluster.size());
                    newSize += newCluster.size();
                    if (cluster == this.nullCluster) {
                        strippedNullCluster = cluster;
                    }
                }
            }
        }

        int newRelationSize = this.relationSize - indices.size();
        double newEntropy = Math.log(newRelationSize) - newKeyGap / newRelationSize;

        sortClusters(newIndex);
        newIndex.trimToSize();

        return new PositionListIndex(newIndex, strippedNullCluster, newSize, newEntropy, newNep, newRelationSize, this.originalRelationSize);
    }

    /**
     * Tests whether to ascending {@link IntList}s are sorted.
     */
    private static boolean isDisjoint(IntList a, IntList b) {
        IntListIterator aIterator = a.iterator();
        IntListIterator bIterator = b.iterator();

        if (!aIterator.hasNext() || !bIterator.hasNext()) return true;
        int nextA = aIterator.nextInt();
        int nextB = bIterator.nextInt();

        while (true) {
            if (nextA == nextB) {
                return false;
            } else if (nextA < nextB) {
                if (!aIterator.hasNext()) break;
                nextA = aIterator.next();
            } else {
                if (!bIterator.hasNext()) break;
                nextB = bIterator.next();
            }
        }

        return true;
    }

    /**
     * Calculate some metric on this instance minus the given {@code indices}.
     *
     * @param indices    that indices to be stripped
     * @param startValue initial value of the {@code accumulator}
     * @param update     invoked for every stripped, non-empty cluster with {@code accumulator} and the cluster size
     * @return the {@code accumulator}
     */
    public double calculateStripped(IntList indices, double startValue, DoubleBinaryOperator update) {
        double accu = startValue;
        for (IntArrayList cluster : this.index) {
            int intersectionSize = getIntersectionSize(cluster, indices);
            int newClusterSize = cluster.size() - intersectionSize;
            if (newClusterSize > 0) accu = update.applyAsDouble(accu, newClusterSize);
        }
        return accu;
    }


    /**
     * Count the number of shared values in to sorted {@link IntList}s.
     */
    private static int getIntersectionSize(IntList a, IntList b) {
        IntListIterator aIterator = a.iterator();
        IntListIterator bIterator = b.iterator();

        if (!aIterator.hasNext() || !bIterator.hasNext()) return 0;
        int nextA = aIterator.nextInt();
        int nextB = bIterator.nextInt();

        int count = 0;
        while (true) {
            if (nextA == nextB) {
                count++;
                if (!aIterator.hasNext() || !bIterator.hasNext()) break;
                nextA = aIterator.next();
                nextB = bIterator.next();
            } else if (nextA < nextB) {
                if (!aIterator.hasNext()) break;
                nextA = aIterator.next();
            } else {
                if (!bIterator.hasNext()) break;
                nextB = bIterator.next();
            }
        }

        return count;
    }


    /**
     * Get the domination value, i.e., order the clusters by their size ascendingly, take the first 50% of values and
     * determine what share of the clusters we find there. That is, for a uniform distribution, we will have 0.5 and
     * smaller values for skewed distributions (> 0).
     *
     * @return the domination value
     */
    public double getDomination() {
        IntArrayList largestCluster = this.getLargestCluster();
        int largestClusterSize = largestCluster.isEmpty() ? 1 : largestCluster.size();
        return largestClusterSize / (double) this.relationSize;
    }

    /**
     * Create a mapping that maps clusters from this instance to the refined clusters in the other instance. That is,
     * a the given instance must refine this instance!
     *
     * @param refined the other, refined instance
     */
    public List<ClusterMapping> mapClustersWith(PositionListIndex refined) {
        List<ClusterMapping> mappings = new LinkedList<>();
        for (IntArrayList cluster : this.index) {
            int numSeenIndices = 0;
            List<IntArrayList> refinedClusters = new ArrayList<>();
            for (IntIterator iterator = cluster.iterator(); iterator.hasNext(); ) {
                int tupleIndex = iterator.nextInt();
                IntArrayList refinedCluster = refined.retrieveClusterByStartIndex(tupleIndex);
                if (refinedCluster != null) {
                    refinedClusters.add(refinedCluster);
                    numSeenIndices += refinedCluster.size();
                }
            }
            mappings.add(new ClusterMapping(cluster, refinedClusters, cluster.size() - numSeenIndices));
        }

        return mappings;
    }

    /**
     * Retrieve the cluster whose first index is given {@code index}.
     *
     * @param index the starting index of the cluster to be retrieved
     * @return the cluster or {@code null} if none
     */
    private IntArrayList retrieveClusterByStartIndex(int index) {
        // Make sure that we have clusters in the first place.
        if (this.index.isEmpty()) return null;

        // Initiate a binary search over the index.
        int lo = 0, hi = this.index.size() - 1;

        // Start by checking whether the interval can contain the index in the first place.
        if (this.index.get(lo).getInt(0) > index || this.index.get(hi).getInt(0) < index) {
            return null;
        }

        // Apply binary search.
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            IntArrayList midCluster = this.index.get(mid);
            int midVal = midCluster.getInt(0);
            if (midVal == index) {
                return midCluster;
            } else if (midVal < index) {
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        IntArrayList loCluster = this.index.get(lo);
        return loCluster.getInt(0) == index ? loCluster : null;
    }

    public int getNumNulls() {
        return this.nullCluster == null ? 0 : this.nullCluster.size();
    }

    public IntArrayList getNullCluster() {
        return this.nullCluster;
    }

    public double getNepWithout(IntArrayList nullCluster) {
        return this.calculateStripped(
                nullCluster,
                0,
                (accu, clusterSize) -> accu + (clusterSize * (clusterSize - 1) / 2)
        );
    }

    public double getNipWithout(IntArrayList indices) {
        return this.getMaximumNipWithout(indices) - this.getNepWithout(indices);
    }

    public double getMaximumNipWithout(IntArrayList strippedIndices) {
        assert this.relationSize == this.originalRelationSize;
        return calculateNep(this.relationSize - strippedIndices.size());
    }

    //    /**
//     * Get the domination value, i.e., order the clusters by their size ascendingly, take the first 50% of values and
//     * determine what share of the clusters we find there. That is, for a uniform distribution, we will have 0.5 and
//     * smaller values for skewed distributions (> 0).
//     * @return the domination value
//     */
//    public double getDomination() {
//        // Retrieve and sort the clusters.
//        List<IntArrayList> clusters = new ArrayList<>(this.index.values());
//        Comparator<IntArrayList> clusterSizeComparator = Comparator.comparingInt(IntArrayList::size);
//        clusters.sort(clusterSizeComparator.reversed());
//
//        double numSeenClusters = 0d;
//        int numSeenIndices = 0;
//        ListIterator<IntArrayList> clusterIterator = clusters.listIterator();
//        int halfRelationSize = this.relationSize / 2;
//        while (clusterIterator.hasNext() && numSeenIndices < halfRelationSize) {
//            IntArrayList cluster = clusterIterator.next();
//            int numTakenIndices = Math.min(cluster.size(), halfRelationSize - numSeenIndices);
//            numSeenClusters += numTakenIndices / (double) cluster.size();
//            numSeenIndices += numTakenIndices;
//        }
//
//        if (numSeenIndices < halfRelationSize) {
//            numSeenClusters += halfRelationSize - numSeenIndices;
//        }
//        double numStrippedClusters = this.relationSize - this.size;
//        return numSeenClusters / (this.index.size() + numStrippedClusters);

    /**
     * Maps an original cluster to refined clusters and counts the refined singleton clusters, which are stripped
     * from this mapping.
     */
    public static class ClusterMapping {

        private final IntArrayList originalCluster;

        private final List<IntArrayList> refinedClusters;

        private final int numRefinedSingletonClusters;

        public ClusterMapping(IntArrayList originalCluster, List<IntArrayList> refinedClusters, int numRefinedSingletonClusters) {
            this.originalCluster = originalCluster;
            this.refinedClusters = refinedClusters;
            this.numRefinedSingletonClusters = numRefinedSingletonClusters;
        }

        public IntArrayList getOriginalCluster() {
            return originalCluster;
        }

        public List<IntArrayList> getRefinedClusters() {
            return refinedClusters;
        }

        public int getNumRefinedSingletonClusters() {
            return numRefinedSingletonClusters;
        }
    }

}
