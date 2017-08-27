package de.hpi.isg.pyro.ducc_dfd;

import de.hpi.isg.pyro.model.Column;
import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;

import java.util.*;

/**
 * This class can detect "holes" in a dependency lattice.
 * <p>The general strategy is to provide this class with maximal non-dependencies. This class will then continuously
 * keep track of the corresponding minimal dependencies. Those minimal dependencies not being actually verified
 * dependencies are then the "holes".</p>
 *
 * @author Jens Ehrlich
 * @author Jakob Zwiener
 * @author Mandy Roick
 * @author Lukas Schulze
 * @author Sebastian Kruse
 */
public class HoleFinder {

    /**
     * Maintains the minimal dependencies according to the seen non-dependencies.
     */
    private Set<Vertical> allegedMinimalDependencies;

    /**
     * Keeps track of the relevant {@link Column}s that may be used for complementation.
     */
    private Vertical fullVertical;

    /**
     * Creates a new instance.
     *
     * @param schema          in which the (non-)dependencies reside
     * @param excludedColumns that must not be used in the (non-)dependencies
     */
    public HoleFinder(RelationSchema schema, Vertical excludedColumns) {
        this.fullVertical = schema.emptyVertical.invert().without(excludedColumns);
        this.allegedMinimalDependencies = new HashSet<>();
        for (Column nonExcludedColumn : excludedColumns.invert().getColumns()) {
            this.allegedMinimalDependencies.add(nonExcludedColumn);
        }
    }

    /**
     * Retrieve "holes", i.e., new seeds according to the known maximal non-dependencies and minimal dependencies.
     *
     * @param minimalDependencies the currently known minimal dependencies
     * @return the hole seeds
     */
    public ArrayList<Vertical> getHoles(Set<Vertical> minimalDependencies) {
        ArrayList<Vertical> holes = new ArrayList<>();

        for (Vertical current : this.allegedMinimalDependencies) {
            if (!minimalDependencies.contains(current)) {
                holes.add(current);
            }
        }

        return holes;
    }

    public void removeMinimalPositivesFromComplementarySet(List<Vertical> sets) {
        for (Vertical singleSet : sets) {
            this.allegedMinimalDependencies.remove(singleSet);
        }
    }

    public void removeMinimalPositiveFromComplementarySet(Vertical set) {
        this.allegedMinimalDependencies.remove(set);
    }

    /**
     * Update the alleged minimum dependencies according to a new maximal non-dependency.
     *
     * @param maximalNegative the maximal non-dependency
     */
    public void updateMaximalNonDependency(Vertical maximalNegative) {
        // Discover all alleged minimal dependencies that are a subset of the non-dependency.
        Collection<Vertical> violatedMinDeps = new ArrayList<>();
        for (Iterator<Vertical> amdIterator = this.allegedMinimalDependencies.iterator(); amdIterator.hasNext(); ) {
            Vertical allegedMinDep = amdIterator.next();
            if (maximalNegative.contains(allegedMinDep)) {
                amdIterator.remove();
            }
            violatedMinDeps.add(allegedMinDep);
        }

        // Escape the violated minimal dependencies and add them back, thereby making sure not to add non-minimal dependencies.
        Column[] escapeColumns = this.fullVertical.without(maximalNegative).getColumns();
        for (Vertical violatedMinDep : violatedMinDeps) {
            // Create any superset of the violated minimum dependency, such that it is not a subset of the non-dependency anymore.
            for (Column escapeColumn : escapeColumns) {
                Vertical escapedMinDep = violatedMinDep.union(escapeColumn);
                // Make sure that the new dependency is actually minimal.
                boolean isMinimum = true;
                for (Vertical allegedMinimalDependency : this.allegedMinimalDependencies) {
                    if (escapedMinDep.contains(allegedMinimalDependency)) {
                        isMinimum = false;
                        break;
                    }
                }
                if (isMinimum) this.allegedMinimalDependencies.add(escapedMinDep);
            }
        }
    }
}
