package de.hpi.isg.pyro.util;

/**
 * Encapsulates two values.
 */
public class Tuple<T1, T2> {

    public T1 f1;

    public T2 f2;

    public Tuple() {
    }

    public Tuple(T1 f1, T2 f2) {
        this.f1 = f1;
        this.f2 = f2;
    }
}
