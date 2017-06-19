package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.ColumnData;
import de.hpi.isg.pyro.model.RelationData;
import de.hpi.isg.pyro.model.Vertical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final VerticalMap<Reference<PositionListIndex>> cache;

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
    }

    /**
     * Obtains a {@link PositionListIndex} for a given {@link Vertical}.
     *
     * @param vertical for which a {@link PositionListIndex} is requested
     * @return the {@link PositionListIndex} or {@code null} if it is not cached
     */
    public PositionListIndex get(Vertical vertical) {
        Reference<PositionListIndex> pliRef = this.cache.get(vertical);
        if (pliRef == null) return null;
        return pliRef.get();
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
        PositionListIndex pli = null;
        Reference<PositionListIndex> pliReference = this.cache.get(vertical);
        if (pliReference != null && (pli = pliReference.get()) != null) {
            if (logger.isDebugEnabled()) logger.debug("Served from PLI cache.");
            return pli;
        }

        // Otherwise, look for cached PLIs from which we can construct the requested PLI.
        ArrayList<Map.Entry<Vertical, Reference<PositionListIndex>>> subsetEntries = this.cache.getSubsetEntries(vertical);

        // Determine the PLI with the smallest extent (and most columns).
        PositionListIndexRank smallestPliRank = null;
        // Greedily take the PLIs with the greatest cover.
        ArrayList<PositionListIndexRank> ranks = new ArrayList<>(subsetEntries.size());
        for (Map.Entry<Vertical, Reference<PositionListIndex>> subsetEntry : subsetEntries) {
            Vertical subVertical = subsetEntry.getKey();
            PositionListIndex subPLI = subsetEntry.getValue().get();
            if (subPLI == null) continue;
            PositionListIndexRank pliRank = new PositionListIndexRank(subVertical, subPLI, subVertical.getArity());
            ranks.add(pliRank);
            if (smallestPliRank == null
                    || smallestPliRank.pli.size() > pliRank.pli.size()
                    || smallestPliRank.pli.size() == pliRank.pli.size() && smallestPliRank.rank < pliRank.rank) {
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
                    rank.rank = coverTester.cardinality();
                    if (rank.rank < 2) {
                        iterator.remove();
                        continue;
                    }

                    if (bestRank == null
                            || bestRank.rank < rank.rank
                            || (bestRank.rank == rank.rank && bestRank.pli.size() > rank.pli.size())) {
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
                    this.cache.put(currentVertical, this.referenceCreation.apply(pli));
                }
            }
        }
        // Cache the PLI.
        if (!this.isCacheIntermediatePlis) {
            this.cache.put(currentVertical, this.referenceCreation.apply(pli));
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

    private static final class PositionListIndexRank {

        static Comparator<PositionListIndexRank> descendingComparator = (r1, r2) -> Integer.compare(r2.rank, r1.rank);

        final Vertical vertical;
        final PositionListIndex pli;
        int rank;

        PositionListIndexRank(Vertical vertical, PositionListIndex pli, int initialRank) {
            this.vertical = vertical;
            this.pli = pli;
            this.rank = initialRank;
        }
    }

}
