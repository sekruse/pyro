package de.hpi.isg.pyro.util;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Interface that mixes {@link java.util.Comparator} and {@link java.io.Serializable}.
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {
}
