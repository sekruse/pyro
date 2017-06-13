package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.Relation;
import de.hpi.isg.pyro.model.Vertical;

import java.lang.ref.Reference;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class caches and provides {@link PositionListIndex}.
 */
public class PLICache {

    private static final boolean isVerbose = false;

    private final Relation relation;

    private final VerticalMap<Reference<PositionListIndex>> cache;

    private boolean isCacheIntermediatePlis = false;

    private final Function<PositionListIndex, Reference<PositionListIndex>> referenceCreation;

    public PLICache(Relation relation,
                    boolean isSynchronized,
                    Function<PositionListIndex, Reference<PositionListIndex>> referenceCreation) {
        this.relation = relation;
        this.cache = isSynchronized ? new SynchronizedVerticalMap<>(this.relation) : new VerticalMap<>(this.relation);
        this.referenceCreation = referenceCreation;
    }

    public PositionListIndex getPositionListIndex(Vertical vertical) {
        if (isVerbose) System.out.printf("PLI for %s requested: ", vertical);

        // Directly ask the vertical to provide the PLI.
        PositionListIndex positionListIndex = vertical.tryGetPositionListIndex();
        if (positionListIndex != null) {
            if (isVerbose) System.out.printf("Served from vertical cache.\n");
            return positionListIndex;
        }

        // See if the PLI is cached.
        Reference<PositionListIndex> positionListIndexReference = this.cache.get(vertical);
        if (positionListIndexReference != null && (positionListIndex = positionListIndexReference.get()) != null) {
            vertical.setPositionListIndex(positionListIndex);
            if (isVerbose) System.out.printf("Served from PLI cache.\n");
            return positionListIndex;
        }

//        System.out.printf("Need to construct PLI for %s. Using: ", vertical);

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
        BitSet cover = new BitSet(this.relation.getNumColumns()), coverTester = new BitSet(this.relation.getNumColumns());
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
//                System.out.printf("%s, ", bestRank.vertical);
                    operands.add(bestRank);
                    cover.or(bestRank.vertical.getColumnIndices());
                }
            }
        }
        // Supply PLIs from columns still missing in the column.
        for (Column column : vertical.getColumns()) {
            if (!cover.get(column.getIndex())) {
//                System.out.printf("%s, ", column);
                operands.add(new PositionListIndexRank(column, column.getPositionListIndex(), 1));
            }
        }
//        System.out.println();

        // Sort the PLIs by their size.
        operands.sort(Comparator.comparing(rank -> rank.pli.size()));
        if (isVerbose)
            System.out.printf("Intersecting %s.\n",
                    operands.stream()
                            .map(rank -> String.format("%s (size=%,d)", rank.vertical, rank.pli.size()))
                            .collect(Collectors.joining(", "))
            );

        // Intersect all the PLIs.
        Vertical currentVertical = null;
        for (PositionListIndexRank operand : operands) {
            if (positionListIndex == null) {
                currentVertical = operand.vertical;
                positionListIndex = operand.pli;
            } else {
                currentVertical = currentVertical.union(operand.vertical);
                positionListIndex = positionListIndex.intersect(operand.pli);
                // Cache the PLI.
                if (this.isCacheIntermediatePlis) {
                    this.cache.put(currentVertical, this.referenceCreation.apply(positionListIndex));
                }
            }
        }
        // Cache the PLI.
        if (!this.isCacheIntermediatePlis) {
            this.cache.put(currentVertical, this.referenceCreation.apply(positionListIndex));
        }
        vertical.setPositionListIndex(positionListIndex);

        if (isVerbose)
            System.out.printf("Calculated from %d sub-PLIs (saved %d intersections).\n", operands.size(), vertical.getArity() - operands.size());

        return positionListIndex;
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
