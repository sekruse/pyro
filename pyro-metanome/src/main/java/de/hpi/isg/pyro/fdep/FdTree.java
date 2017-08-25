package de.hpi.isg.pyro.fdep;

import java.util.*;

/**
 * This is an implementation of the FD-Tree as described in the FDEP paper. The implementation is closely guided by
 * the original FDEP implementation.
 */
public class FdTree {

    /**
     * The number of attributes that can be stored in this instance.
     */
    private final int numAttributes;

    /**
     * The root {@link Node}.
     */
    private final Node root;

    /**
     * Creates a new instance.
     *
     * @param numAttributes the number of attributes that can be stored in the new instance
     */
    public FdTree(int numAttributes) {
        this.numAttributes = numAttributes;
        this.root = new Node();
    }

    /**
     * Adds (a single occurrence of) a new FD.
     *
     * @param lhs the attributes (expressed by integers) of the FD's LHS
     * @param lhs the attributes (expressed by integers) of the FD's LHS
     */
    public void add(BitSet lhs, BitSet rhs) {
        this.root.add(lhs, 0, rhs, 1L);
    }

    /**
     * Adds a new FD.
     *
     * @param lhs            the attributes (expressed by integers) of the FD's LHS
     * @param lhs            the attributes (expressed by integers) of the FD's LHS
     * @param numOccurrences occurrences for the FD (e.g., violations of a non-FD)
     */
    public void add(BitSet lhs, BitSet rhs, long numOccurrences) {
        this.root.add(lhs, 0, rhs, 1L);
    }

    /**
     * Checks if this instance contains the FD with the given {@code lhs} and {@code rhs} or a specification, respectively.
     *
     * @param lhs the FD's LHS in terms of attribute indices
     * @param rhs the FD's RHS as attribute index
     * @return whether this instance contains any said FD
     */
    public boolean containsSpecification(BitSet lhs, int rhs) {
        return this.root.containsSpecification(lhs, 0, rhs);
    }

    /**
     * Checks if this instance contains the FD with the given {@code lhs} and {@code rhs} or a generalization, respectively.
     *
     * @param lhs the FD's LHS in terms of attribute indices
     * @param rhs the FD's RHS as attribute index
     * @return whether this instance contains any said FD
     */
    public boolean containsGeneralization(BitSet lhs, int rhs) {
        return this.root.containsGeneralization(lhs, 0, rhs);
    }

    /**
     * Counts the occurrences of the FD with the given {@code lhs} and {@code rhs} and any specification, respectively.
     *
     * @param lhs the FD's LHS in terms of attribute indices
     * @param rhs the FD's RHS as attribute index
     * @return the counted occurrences
     */
    public long countSpecificationOccurrences(BitSet lhs, int rhs) {
        return this.root.countSpecificationOccurrences(lhs, 0, rhs);
    }

    /**
     * Removes FDs generalizing {@code lhs} &#8594; {@code rhs}.
     *
     * @param lhs the FD's LHS in terms of attribute indices
     * @param rhs the FD's RHS as attribute index
     * @return whether this instance contains any said FD
     */
    public Collection<BitSet> removeGeneralizations(BitSet lhs, int rhs) {
        Collection<BitSet> specializations = new ArrayList<>();
        this.root.removeGeneralizations(lhs, new BitSet(this.numAttributes), rhs, specializations);
        return specializations;
    }

    /**
     * Creates a new instance that contains only those (non-)FDs that
     * <ol>
     * <li>have at least {@code minOccurrences} and</li>
     * <li>are maximal in that respect (tailored to non-FDs).</li>
     * </ol>
     * The number of occurrences are copied, too.
     *
     * @param minOccurrences the minimum number of occurrences of a (non-)FD so that it is not pruned
     * @return a new instance without the pruned (non-)FDs
     */
    public FdTree prune(long minOccurrences) {
        FdTree prunedTree = new FdTree(this.numAttributes);
        BitSet currentAttributes = new BitSet(this.numAttributes);
        this.root.makePrunedCopy(prunedTree, minOccurrences, currentAttributes);
        return prunedTree;
    }

    /**
     * Returns the LHS of all FDs in this instance with the given {@code rhs}.
     *
     * @param rhs the index of the RHS attribute
     * @return an {@link Iterable} with all LHS of the requested FDs
     */
    public Iterable<BitSet> getAllLhs(int rhs) {
        return () -> new Iterator<BitSet>() {

            /**
             * The next LHS to return.
             */
            private BitSet next;

            /**
             * Currently visited {@link Node}s.
             */
            private Stack<Node> currentNodes = new Stack<>();

            /**
             * Attribute indices of the the {@link #currentNodes}.
             */
            private BitSet currentAttributes = new BitSet(FdTree.this.numAttributes);


            // "Constructor".
            {
                if (FdTree.this.root.hasFd[rhs]) {
                    this.currentNodes.push(FdTree.this.root);
                    if (FdTree.this.root.occurrences[rhs] > 0) {
                        this.next = new BitSet();
                    } else {
                        this.moveToNext();
                    }
                }
            }

            /**
             * Set {@link #next} to the next element.
             */
            private void moveToNext() {
                int nextAttribute = this.currentAttributes.length();

                while (true) {
                    Node currentNode = this.currentNodes.peek();

                    // Visit children to find the next LHS.
                    for (; nextAttribute < FdTree.this.numAttributes; nextAttribute++) {
                        Node child = currentNode.children[nextAttribute];
                        if (child != null && child.hasFd[rhs]) {
                            this.currentNodes.push(child);
                            this.currentAttributes.set(nextAttribute);
                            currentNode = child;
                            if (currentNode.occurrences[rhs] > 0) {
                                this.next = (BitSet) this.currentAttributes.clone();
                                return;
                            }
                        }
                    }

                    // If we did not find an LHS as specification, then we need to go back to the parents.
                    if (this.currentNodes.size() == 1) {
                        this.next = null;
                        return;
                    }
                    this.currentNodes.pop();
                    nextAttribute = this.currentAttributes.length();
                    this.currentAttributes.clear(nextAttribute - 1);
                }
            }

            @Override
            public boolean hasNext() {
                return this.next != null;
            }

            @Override
            public BitSet next() {
                if (this.next == null) throw new NoSuchElementException();
                BitSet next = this.next;
                this.moveToNext();
                return next;
            }
        };
    }


    /**
     * Represents a node of an {@link FdTree}.
     */
    private final class Node {

        /**
         * The number of occurrences counted along this node for every attribute.
         */
        private final long[] occurrences = new long[FdTree.this.numAttributes];

        /**
         * Tells whether this instance or any children contains an FD with the given RHS (index).
         */
        private final boolean[] hasFd = new boolean[FdTree.this.numAttributes];

        /**
         * Child {@link Node}s.
         */
        private final Node[] children = new Node[FdTree.this.numAttributes];

        /**
         * Add an FD to this and its appropriate children.
         *
         * @param lhs         the attributes (expressed by integers) of the FD's LHS
         * @param lhsIndex    the attribute with an index equal or higher to this value determines the next child to go;
         *                    if none, this instance is terminal
         * @param rhs         the attributes (expressed by integers) of the FD's RHS
         * @param occurrences number of occurrences (for non-FD violations)
         */
        private void add(BitSet lhs, int lhsIndex, BitSet rhs, long occurrences) {
            // Determine the next node.
            final int nextAttribute = lhs.nextSetBit(lhsIndex);
            final boolean isTerminal = nextAttribute == -1;

            // Update counts and FDs.
            for (int rhsAttr = rhs.nextSetBit(0); rhsAttr != -1; rhsAttr = rhs.nextSetBit(rhsAttr + 1)) {
                this.hasFd[rhsAttr] = true;
                if (isTerminal) {
                    this.occurrences[rhsAttr] += occurrences;
                }
            }

            // Update the children.
            if (!isTerminal) {
                Node child = this.children[nextAttribute];
                if (child == null) child = this.children[nextAttribute] = new Node();
                child.add(lhs, nextAttribute + 1, rhs, occurrences);
            }
        }

        /**
         * Tests whether the given FD or a specification of the same is contained in this instance.
         *
         * @param lhs      the attributes (expressed by integers) of the FD's LHS
         * @param lhsIndex the attribute with an index equal or higher to this value determines the next child to go;
         *                 if none, this instance is terminal;
         *                 this value also is the minimum interesting next attribute index to look at, i.e., all smaller
         *                 attributes cannot exist in this node
         * @param rhs      the attributes (expressed by an integer) of the FD's RHS
         * @return whether this instance or any child contains a specification FD
         */
        private boolean containsSpecification(BitSet lhs, int lhsIndex, int rhs) {
            // Check if we are on a dead end.
            if (!this.hasFd[rhs]) return false;

            // Determine the next node.
            final int nextAttribute = lhsIndex == -1 ? -1 : lhs.nextSetBit(lhsIndex);
            final boolean isTerminal = nextAttribute == -1;

            // If we have seen all LHS attributes, we can check if we have an FD.
            if (isTerminal && this.occurrences[rhs] > 0) return true;

            // If we have not yet found a specialization, we need to resort to the children.
            int maxAttribute = nextAttribute == -1 ? FdTree.this.numAttributes : nextAttribute + 1;
            for (; lhsIndex < maxAttribute; lhsIndex++) {
                Node child = this.children[lhsIndex];
                if (child != null) {
                    if (child.containsSpecification(lhs, lhsIndex + 1, rhs)) return true;
                }
            }

            // If we have not found anything...
            return false;
        }

        /**
         * Tests whether the given FD or a generalization of the same is contained in this instance.
         *
         * @param lhs      the attributes (expressed by integers) of the FD's LHS
         * @param lhsIndex the attribute with an index equal or higher to this value determines the next child to go;
         *                 if none, this instance is terminal;
         *                 this value also is the minimum interesting next attribute index to look at, i.e., all smaller
         *                 attributes cannot exist in this node
         * @param rhs      the attributes (expressed by an integer) of the FD's RHS
         * @return whether this instance or any child contains a specification FD
         */
        private boolean containsGeneralization(BitSet lhs, int lhsIndex, int rhs) {
            // First of all, check this very node, which must be a generalization of the given lhs.
            if (this.occurrences[rhs] > 0) return true;

            // Check if we are on a dead end.
            if (!this.hasFd[rhs]) return false;

            // Check the children.
            for (int nextAttribute = lhs.nextSetBit(lhsIndex);
                 nextAttribute != -1;
                 nextAttribute = lhs.nextSetBit(nextAttribute + 1)) {
                Node child = this.children[nextAttribute];
                if (child != null && child.containsSpecification(lhs, nextAttribute + 1, rhs)) return true;
            }

            // If we have not found anything...
            return false;
        }

        /**
         * Count the number of occurrences of a given FD and all its specifications.
         *
         * @param lhs      the attributes (expressed by integers) of the FD's LHS
         * @param lhsIndex the attribute with an index equal or higher to this value determines the next child to go;
         *                 if none, this instance is terminal;
         *                 this value also is the minimum interesting next attribute index to look at, i.e., all smaller
         *                 attributes cannot exist in this node
         * @param rhs      the attributes (expressed by an integer) of the FD's RHS
         * @return the number of stored occurrences
         */
        private long countSpecificationOccurrences(BitSet lhs, int lhsIndex, int rhs) {
            // Check if we are on a dead end.
            if (!this.hasFd[rhs]) return 0L;

            // Determine the next node.
            final int nextAttribute = lhsIndex == -1 ? -1 : lhs.nextSetBit(lhsIndex);
            final boolean isTerminal = nextAttribute == -1;

            // Possibly count the occurrences in this node.
            long count = isTerminal ? this.occurrences[rhs] : 0L;

            // Count the occurrences in all relevant children.
            int maxAttribute = nextAttribute == -1 ? FdTree.this.numAttributes : nextAttribute + 1;
            for (; lhsIndex < maxAttribute; lhsIndex++) {
                Node child = this.children[lhsIndex];
                if (child != null) {
                    count += child.countSpecificationOccurrences(lhs, lhsIndex + 1, rhs);
                }
            }

            // Return the full count.
            return count;
        }

        /**
         * Removes FDs generalizing {@code lhs} &#8594; {@code rhs}.
         *
         * @param lhs             the FD's LHS in terms of attribute indices
         * @param rhs             the FD's RHS as attribute index
         * @param specializations collects the LHS of any removed FD
         */
        private void removeGeneralizations(BitSet lhs, BitSet currentAttributes, int rhs, Collection<BitSet> specializations) {
            // Check if we are on a dead end.
            if (!this.hasFd[rhs]) return;

            // First, remove all generalizations within child nodes.
            boolean isChildHasFd = false;
            for (int nextAttribute = lhs.nextSetBit(currentAttributes.length());
                 nextAttribute != -1;
                 nextAttribute = lhs.nextSetBit(nextAttribute + 1)) {
                Node child = this.children[nextAttribute];
                if (child != null) {
                    currentAttributes.set(nextAttribute);
                    child.removeGeneralizations(lhs, currentAttributes, rhs, specializations);
                    // TODO: Actually remove nodes.
                    currentAttributes.clear(nextAttribute);
                    isChildHasFd |= child.hasFd[rhs];
                }
            }

            // Now potentially remove the FD at this very instance.
            if (this.occurrences[rhs] > 0) {
                this.occurrences[rhs] = 0;
                specializations.add((BitSet) currentAttributes.clone());
            }

            // Remove hints on FDs if we removed them all.
            if (!isChildHasFd && this.occurrences[rhs] == 0) this.hasFd[rhs] = false;
        }

        /**
         * Copies the contents of this instance to a new {@link FdTree}, thereby pruning non-maximal (non-)FDs and those
         * with occurrences less than {@code minOccurrences}.
         *
         * @param prunedTree        the {@link FdTree} to copy to
         * @param minOccurrences    the minimum number of occurrences for instances to be copied
         * @param currentAttributes current attributes (of this instance and all parents)
         */
        private void makePrunedCopy(FdTree prunedTree, long minOccurrences, BitSet currentAttributes) {
            // First, copy the content of children.
            for (int i = 0; i < this.children.length; i++) {
                Node child = this.children[i];
                if (child != null) {
                    currentAttributes.set(i);
                    child.makePrunedCopy(prunedTree, minOccurrences, currentAttributes);
                    currentAttributes.clear(i);
                }
            }

            // Copy (non-)FDs of this node.
            for (int rhs = 0; rhs < this.children.length; rhs++) {
                // Check that we have a non-redundant (non-)FD.
                if (this.occurrences[rhs] > 0 && !prunedTree.containsSpecification(currentAttributes, rhs)) {
                    // Count the number of violations of that particular (non-)FD.
                    long numViolations = FdTree.this.countSpecificationOccurrences(currentAttributes, rhs);
                    // Add the non-FD if it has sufficient violations.
                    if (numViolations >= minOccurrences) {
                        BitSet rhsBitSet = new BitSet();
                        rhsBitSet.set(rhs);
                        prunedTree.add((BitSet) currentAttributes.clone(), rhsBitSet, numViolations);
                    }
                }
            }
        }
    }
}

