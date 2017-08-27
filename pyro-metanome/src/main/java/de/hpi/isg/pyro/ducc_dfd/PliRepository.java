package de.hpi.isg.pyro.ducc_dfd;

import de.hpi.isg.pyro.model.*;
import de.hpi.isg.pyro.util.PositionListIndex;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.*;

/**
 * Repository for {@link PositionListIndex}es.
 *
 * @author Patrick Schulze
 * @author Sebastian Kruse
 */
public class PliRepository {

    /**
     * The maximum number of {@link PositionListIndex}es to cache or {@code -1} to have no limit.
     */
    private final int capacity;

    /**
     * The number of least recently used {@link PositionListIndex}es that will not be evicted from the cache on clean up.
     */
    private final int numProtectedLruPartitions;

    /**
     * The {@link RelationSchema} of the cached {@link PositionListIndex}es and their {@link Vertical}s.
     */
    private final RelationSchema schema;

    /**
     * Stores the actual {@link PositionListIndex}es.
     */
    private final Int2ObjectMap<Map<Vertical, PositionListIndex>> cache = new Int2ObjectOpenHashMap<>();

    /**
     * Keeps track of how often a {@link PositionListIndex} for a certain vertical has been used.
     */
    private Object2IntOpenHashMap<Vertical> usageCounter;

    /**
     * Maintains the order of the least recently used {@link Vertical}s.
     */
    private LinkedList<Vertical> leastRecentlyUsedPartitions;

    /**
     * Create a new instance.
     *
     * @param relation                  the loaded relation
     * @param capacity                  the maximum number of {@link PositionListIndex}es to cache or {@code -1} to have
     *                                  no limit
     * @param numProtectedLruPartitions the number of least recently used {@link PositionListIndex}es that will not be
     *                                  evicted from the cache on clean up
     */
    public PliRepository(ColumnLayoutRelationData relation, int capacity, int numProtectedLruPartitions) {
        this.capacity = capacity;
        this.numProtectedLruPartitions = numProtectedLruPartitions;
        this.schema = relation.getSchema();
        this.usageCounter = new Object2IntOpenHashMap<>();
        this.leastRecentlyUsedPartitions = new LinkedList<>();
        for (int cardinality = 1; cardinality <= this.schema.getNumColumns(); cardinality++) {
            this.cache.put(cardinality, new HashMap<>());
        }
        // Store the single column PLIs.
        for (ColumnData columnData : relation.getColumnData()) {
            this.cache.get(1).put(columnData.getColumn(), columnData.getPositionListIndex());
            this.usageCounter.put(columnData.getColumn(), 0);
        }
    }

    private boolean isUseMemoryManagement() {
        return this.capacity > 0;
    }

    /**
     * @return the number of stored {@link PositionListIndex}es.
     */
    public int size() {
        return this.usageCounter.size();
    }

    /**
     * Retrieve the {@link PositionListIndex} associated to the {@link Column} with the given index.
     *
     * @param columnIndex the index
     * @return the {@link PositionListIndex} or {@code null} if none
     */
    public PositionListIndex get(int columnIndex) {
        return this.get(this.schema.getColumn(columnIndex));
    }

    /**
     * Retrieve the {@link PositionListIndex} associated to the given {@link Vertical}.
     *
     * @param vertical for which a {@link PositionListIndex} is requested
     * @return the {@link PositionListIndex} or {@code null} if none
     */
    public PositionListIndex get(Vertical vertical) {
        Map<Vertical, PositionListIndex> cachePartition = this.cache.get(vertical.getArity());
        assert cachePartition != null : String.format("No cache partition for %s.", vertical);
        PositionListIndex result = cachePartition.get(vertical);
        if (result != null) this.notifyUsage(vertical);
        return result;
    }

    private void notifyUsage(Vertical vertical) {
        if (this.isUseMemoryManagement()) {
            this.usageCounter.addTo(vertical, 1);
            this.leastRecentlyUsedPartitions.add(vertical);
            if (this.leastRecentlyUsedPartitions.size() > this.numProtectedLruPartitions) {
                this.leastRecentlyUsedPartitions.removeFirst();
            }
        }
    }

    /**
     * Cache the given {@link PositionListIndex}, associating it with the given {@link Vertical}.
     *
     * @param vertical the {@link Vertical}
     * @param pli      the {@link PositionListIndex}
     */
    public void cache(Vertical vertical, PositionListIndex pli) {
        int arity = vertical.getArity();
        this.cache.get(arity).put(vertical, pli);
        this.notifyUsage(vertical);
        this.freeSpace();
    }

    /**
     * Remove some cached {@link PositionListIndex}. In fact, those are removed that are used less than the median usage.
     */
    private void freeSpace() {
        if (this.isUseMemoryManagement() && this.size() > this.capacity + this.schema.getNumColumns()) {
            // Get the median partition usage.
            int[] usageCounters = this.usageCounter.values().toIntArray();
            Arrays.sort(usageCounters);
            int medianOfUsage = usageCounters[usageCounters.length / 2];
            if (usageCounters.length % 2 == 0) {
                medianOfUsage += usageCounters[usageCounters.length / 2 + 1];
                medianOfUsage /= 2;
            }

            // Remove all partitions used less than the median but spare protected partitions.
            Set<Vertical> protectedVerticals = new HashSet<>(this.leastRecentlyUsedPartitions);
            for (ObjectIterator<Object2IntMap.Entry<Vertical>> usageCounterIterator = this.usageCounter.object2IntEntrySet().fastIterator();
                 usageCounterIterator.hasNext(); ) {
                Object2IntMap.Entry<Vertical> counterEntry = usageCounterIterator.next();
                Vertical vertical = counterEntry.getKey();
                if (vertical.getArity() == 1
                        || counterEntry.getIntValue() > medianOfUsage
                        || protectedVerticals.contains(vertical)) {
                    continue;
                }
                this.cache.get(vertical.getArity()).remove(vertical);
            }

            // Finally, reset the usage counter.
            this.usageCounter.clear();
            for (Map<Vertical, PositionListIndex> cachePartition : this.cache.values()) {
                for (Vertical vertical : cachePartition.keySet()) {
                    this.usageCounter.put(vertical, 0);
                }
            }
        }
    }

    /**
     * Retrieve a cover of {@link PositionListIndex}es for the given {@link Vertical}.
     *
     * @param vertical for which {@link PositionListIndex}es are requested
     * @return the {@link PositionListIndex}es
     */
    public ArrayList<PositionListIndex> getCover(Vertical vertical) {
        ArrayList<PositionListIndex> bestMatchingPartitions = new ArrayList<>(vertical.getArity());
        Vertical uncoveredColumns = vertical;
        int sizeOfLastMatch = uncoveredColumns.getArity();

        // the strategy is greedy and fit first
        outer:
        while (uncoveredColumns.getArity() > 0) {
            // we don't need to check the sizes above the last match size again
            for (int searchArity = Math.min(uncoveredColumns.getArity(), sizeOfLastMatch);
                 searchArity > 0;
                 searchArity--) {

                Map<Vertical, PositionListIndex> cachePartition = this.cache.get(searchArity);
                for (Map.Entry<Vertical, PositionListIndex> entry : cachePartition.entrySet()) {
                    Vertical entryKey = entry.getKey();
                    if (vertical.contains(entryKey) && entryKey.intersects(uncoveredColumns)) {
                        bestMatchingPartitions.add(entry.getValue());
                        uncoveredColumns = uncoveredColumns.without(entry.getKey());
                        sizeOfLastMatch = searchArity;
                        this.notifyUsage(entryKey);
                        continue outer;
                    }
                }
            }
        }
        return bestMatchingPartitions;
    }

    /**
     * Retrieves a {@link PositionListIndex} for the given {@link Vertical}. If it is not cached, it will be created
     * from a {@link #getCover(Vertical) cover} of existing {@link PositionListIndex}es and cached.
     *
     * @param vertical for which a {@link PositionListIndex} is requested
     * @return the {@link PositionListIndex}
     */
    public PositionListIndex getOrCalculateAndCache(Vertical vertical) {
        PositionListIndex positionListIndex = this.get(vertical);
        if (positionListIndex != null) return positionListIndex;
        ArrayList<PositionListIndex> positionListIndices = this.getCover(vertical);
        Iterator<PositionListIndex> pliIterator = positionListIndices.iterator();
        positionListIndex = pliIterator.next();
        while (pliIterator.hasNext()) {
            positionListIndex = positionListIndex.intersect(pliIterator.next());
        }
        this.cache(vertical, positionListIndex);
        return positionListIndex;
    }
}
