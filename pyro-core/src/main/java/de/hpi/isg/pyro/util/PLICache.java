package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnData;
import de.hpi.isg.pyro.model.RelationData;
import de.hpi.isg.pyro.model.Vertical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.ref.Reference;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class caches and provides {@link PositionListIndex}.
 */
public class PLICache {

    private static final Logger logger = LoggerFactory.getLogger(PLICache.class);

    private final RelationData relationData;

    private final VerticalMap<PositionListIndex> cache;

    private final MemoryWatchdog memoryWatchdog;

    private boolean isCacheIntermediatePlis = false;

    private final Function<PositionListIndex, Reference<PositionListIndex>> referenceCreation;

    public PLICache(RelationData relationData,
                    boolean isSynchronized,
                    Function<PositionListIndex, Reference<PositionListIndex>> referenceCreation) {
        this.relationData = relationData;
        this.cache = isSynchronized ?
                new SynchronizedVerticalMap<>(this.relationData.getSchema()) :
                new VerticalMap<>(this.relationData.getSchema());
        this.referenceCreation = referenceCreation;

        // Set up monitoring for cache clean up.
        this.memoryWatchdog = new MemoryWatchdog(0.85, 100);
//        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
//        ((NotificationEmitter) memoryMXBean).addNotificationListener(
//                (notification, handback) -> System.out.printf("Received %s with handback %s.\n", notification, handback),
//                (NotificationFilter) notification -> true,
//                this
//        );
//        for (MemoryPoolMXBean memoryPoolMXBean : ManagementFactory.getMemoryPoolMXBeans()) {
//            System.out.printf("We are having a Bean named \"%s\" (\"%s\").\n", memoryPoolMXBean.getName(), memoryPoolMXBean.getType());
//        }
//
//        for (MemoryManagerMXBean bean : ManagementFactory.getMemoryManagerMXBeans()) {
//            System.out.printf("We are having a Bean named \"%s\".\n", bean.getName());
//        }

    }

    /**
     * Obtains a {@link PositionListIndex} for a given {@link Vertical}.
     *
     * @param vertical for which a {@link PositionListIndex} is requested
     * @return the {@link PositionListIndex} or {@code null} if it is not cached
     */
    public PositionListIndex get(Vertical vertical) {
        return this.cache.get(vertical);
    }

    /**
     * Obtains a {@link PositionListIndex} for a given {@link Vertical}. If it is not cached, it will be calculated
     * using cached {@link PositionListIndex}es and, eventually, cached.
     *
     * @param vertical for which a {@link PositionListIndex} is required
     * @return the {@link PositionListIndex}
     */
    public PositionListIndex getOrCreateFor(Vertical vertical) {
        if (logger.isDebugEnabled()) logger.debug("PLI for {} requested: ", vertical);

        // See if the PLI is cached.
        PositionListIndex pli = this.get(vertical);
        if (pli != null) {
            if (logger.isDebugEnabled()) logger.debug("Served from PLI cache.");
            return pli;
        }

        // Otherwise, look for cached PLIs from which we can construct the requested PLI.
        ArrayList<Map.Entry<Vertical, PositionListIndex>> subsetEntries = this.cache.getSubsetEntries(vertical);

        // Determine the PLI with the smallest extent (and most columns).
        PositionListIndexRank smallestPliRank = null;
        // Greedily take the PLIs with the greatest cover.
        ArrayList<PositionListIndexRank> ranks = new ArrayList<>(subsetEntries.size());
        for (Map.Entry<Vertical, PositionListIndex> subsetEntry : subsetEntries) {
            Vertical subVertical = subsetEntry.getKey();
            PositionListIndex subPLI = subsetEntry.getValue();
            PositionListIndexRank pliRank = new PositionListIndexRank(subVertical, subPLI, subVertical.getArity());
            ranks.add(pliRank);
            if (smallestPliRank == null
                    || smallestPliRank.pli.size() > pliRank.pli.size()
                    || smallestPliRank.pli.size() == pliRank.pli.size() && smallestPliRank.addedArity < pliRank.addedArity) {
                smallestPliRank = pliRank;
            }
        }
        LinkedList<PositionListIndexRank> operands = new LinkedList<>();
        BitSet cover = new BitSet(this.relationData.getNumColumns()), coverTester = new BitSet(this.relationData.getNumColumns());
        if (smallestPliRank != null) {
            operands.add(smallestPliRank);
            cover.or(smallestPliRank.vertical.getColumnIndices());

            while (cover.cardinality() < vertical.getArity() && !ranks.isEmpty()) {
                PositionListIndexRank bestRank = null;

                for (Iterator<PositionListIndexRank> iterator = ranks.iterator(); iterator.hasNext(); ) {
                    PositionListIndexRank rank = iterator.next();
                    coverTester.clear();
                    coverTester.or(rank.vertical.getColumnIndices());
                    coverTester.andNot(cover);
                    rank.addedArity = coverTester.cardinality();
                    if (rank.addedArity < 2) {
                        iterator.remove();
                        continue;
                    }

                    if (bestRank == null
                            || bestRank.addedArity < rank.addedArity
                            || (bestRank.addedArity == rank.addedArity && bestRank.pli.size() > rank.pli.size())) {
                        bestRank = rank;
                    }
                }

                if (bestRank != null) {
                    operands.add(bestRank);
                    cover.or(bestRank.vertical.getColumnIndices());
                }
            }
        }
        // Supply PLIs from columns still missing in the column.
        for (Column column : vertical.getColumns()) {
            if (!cover.get(column.getIndex())) {
                ColumnData columnData = this.relationData.getColumnData(column.getIndex());
                operands.add(new PositionListIndexRank(column, columnData.getPositionListIndex(), 1));
            }
        }

        // Sort the PLIs by their size.
        operands.sort(Comparator.comparing(rank -> rank.pli.size()));
        if (logger.isDebugEnabled())
            logger.debug("Intersecting {}.",
                    operands.stream()
                            .map(rank -> String.format("%s (size=%,d)", rank.vertical, rank.pli.size()))
                            .collect(Collectors.joining(", "))
            );

        // Intersect all the PLIs.
        Vertical currentVertical = null;
        for (PositionListIndexRank operand : operands) {
            if (pli == null) {
                currentVertical = operand.vertical;
                pli = operand.pli;
            } else {
                currentVertical = currentVertical.union(operand.vertical);
                pli = pli.intersect(operand.pli);
                // Cache the PLI.
                if (this.isCacheIntermediatePlis) {
                    this.cache.put(currentVertical, pli);
                }
            }
        }
        // Cache the PLI.
        if (!this.isCacheIntermediatePlis) {
            this.cache.put(currentVertical, pli);
        }

        if (logger.isDebugEnabled())
            logger.debug("Calculated from {} sub-PLIs (saved {} intersections).\n", operands.size(), vertical.getArity() - operands.size());

        return pli;
    }

    public int size() {
        return this.cache.size();
    }

    public boolean isCacheIntermediatePlis() {
        return isCacheIntermediatePlis;
    }

    public void setCacheIntermediatePlis(boolean cacheIntermediatePlis) {
        isCacheIntermediatePlis = cacheIntermediatePlis;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        this.memoryWatchdog.stop();
    }

    private static final class PositionListIndexRank {

        final Vertical vertical;
        final PositionListIndex pli;
        int addedArity;

        PositionListIndexRank(Vertical vertical, PositionListIndex pli, int initialArity) {
            this.vertical = vertical;
            this.pli = pli;
            this.addedArity = initialArity;
        }
    }

    private final class MemoryWatchdog implements Runnable {

        boolean keepRunning = true;

        final long sleepMillis;

        final double maxUsageRatio;

        final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        final Thread thread;

        final Logger logger = LoggerFactory.getLogger(this.getClass());

        private MemoryWatchdog(double maxUsageRatio, long sleepMillis) {
            this.maxUsageRatio = maxUsageRatio;
            this.sleepMillis = sleepMillis;
            this.thread = new Thread(this);
            this.thread.start();
        }

        @Override
        public void run() {
            while (this.keepRunning) {
                MemoryUsage heapMemoryUsage = this.memoryMXBean.getHeapMemoryUsage();
                if (heapMemoryUsage.getUsed() > this.maxUsageRatio * heapMemoryUsage.getMax()) {
                    int initialCacheSize = cache.size();
                    long elapsedNanos = System.nanoTime();
                    cache.shrink(
                            0.5d,
                            Comparator.comparingInt(entry -> entry.getKey().getArity()),
                            entry -> entry.getKey().getArity() > 1
                    );
                    elapsedNanos = System.nanoTime() - elapsedNanos;
                    this.logger.info(String.format("Shrinked PLI cache size from %,d to %,d in %,d ms.",
                            initialCacheSize, cache.size(), elapsedNanos / 1_000_000
                    ));
                }
                try {
                    Thread.sleep(this.sleepMillis);
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
            this.logger.info("Terminating...");
        }

        void stop() {
            this.keepRunning = false;
            this.thread.interrupt();
        }
    }

}
