package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.Vertical;

/**
 * This class hosts pruning information on {@link Vertical}s. This information has to be interpreted in the light of
 * some of some (implicit) {@link DependencyStrategy}.
 */
public class VerticalInfo {

    /**
     * Whether the described {@link Vertical} forms a dependency.
     */
    boolean isDependency;

    /**
     * Whether the described {@link Vertical} forms an extremal (non-)dependency.
     */
    boolean isExtremal;

    /**
     * The calculated error of the described {@link Vertical}.
     */
    double error;

    /**
     * Creates a new instance for a non-minimal dependency.
     *
     * @return the new instance
     */
    static VerticalInfo forDependency() {
        return new VerticalInfo(true, false);
    }

    /**
     * Creates a new instance for a minimal dependency.
     *
     * @return the new instance
     */
    static VerticalInfo forMinimalDependency() {
        return new VerticalInfo(true, true);
    }

    /**
     * Creates a new instance for a non-maximal non-dependency.
     *
     * @return the new instance
     */
    static VerticalInfo forNonDependency() {
        return new VerticalInfo(false, false);
    }

    /**
     * Creates a new instance for a maximal non-dependency.
     *
     * @return the new instance
     */
    static VerticalInfo forMaximalNonDependency() {
        return new VerticalInfo(false, true);
    }

    /**
     * Creates a new instance.
     *
     * @param isDependency see {@link #isDependency}
     * @param isExtremal   see {@link #isExtremal}
     */
    VerticalInfo(boolean isDependency, boolean isExtremal) {
        this(isDependency, isExtremal, Double.NaN);
    }

    /**
     * Creates a new instance.
     *
     * @param isDependency see {@link #isDependency}
     * @param isExtremal   see {@link #isExtremal}
     * @param error        see {@link #error}
     */
    VerticalInfo(boolean isDependency, boolean isExtremal, double error) {
        this.isDependency = isDependency;
        this.isExtremal = isExtremal;
        this.error = error;
    }

    /**
     * Whether the described {@link Vertical} is determining the state (namely being a dependency) of super-{@link Vertical}s.
     *
     * @return whether said condition is satisfied
     */
    boolean isPruningSupersets() {
        return this.isDependency || this.isExtremal;
    }

    /**
     * Whether the described {@link Vertical} is determining the state (namely being a non-dependency) of sub-{@link Vertical}s.
     *
     * @return whether said condition is satisfied
     */
    boolean isPruningSubsets() {
        return !this.isDependency || this.isExtremal;
    }

    @Override
    public String toString() {
        if (this.isDependency) {
            return this.isExtremal ? "minimal dependency" : "dependency";
        } else {
            return this.isExtremal ? "maximal non-dependency" : "non-dependency";
        }
    }

}
