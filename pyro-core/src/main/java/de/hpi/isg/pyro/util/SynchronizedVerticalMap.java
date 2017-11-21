package de.hpi.isg.pyro.util;

import de.hpi.isg.pyro.model.RelationSchema;
import de.hpi.isg.pyro.model.Vertical;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

/**
 * This class uses a trie to map {@link Vertical}s to values.
 */
public class SynchronizedVerticalMap<Value> extends VerticalMap<Value> {

    public final ReadWriteLock lock = new ReentrantReadWriteLock(true);

    public SynchronizedVerticalMap(RelationSchema relation) {
        super(relation);
    }

    @Override
    public int size() {
        try {
            this.lock.readLock().lock();
            return super.size();
        } finally {
            this.lock.readLock().unlock();

        }
    }

    @Override
    public boolean isEmpty() {
        try {
            this.lock.readLock().lock();
            return super.isEmpty();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        try {
            this.lock.readLock().lock();
            return super.containsKey(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        try {
            this.lock.readLock().lock();
            return super.containsValue(value);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public Value get(Object key) {
        try {
            this.lock.readLock().lock();
            return super.get(key);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public Value put(Vertical key, Value value) {
        try {
            this.lock.writeLock().lock();
            return super.put(key, value);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public Value remove(Object key) {
        try {
            this.lock.writeLock().lock();
            return super.remove(key);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(Map<? extends Vertical, ? extends Value> m) {
        try {
            this.lock.writeLock().lock();
            super.putAll(m);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        try {
            this.lock.writeLock().lock();
            super.clear();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    @Override
    public ArrayList<Vertical> getSubsetKeys(Vertical vertical) {
        try {
            this.lock.readLock().lock();
            return super.getSubsetKeys(vertical);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public ArrayList<Entry<Vertical, Value>> getSubsetEntries(Vertical vertical) {
        try {
            this.lock.readLock().lock();
            return super.getSubsetEntries(vertical);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public ArrayList<Entry<Vertical, Value>> getSupersetEntries(Vertical vertical) {
        try {
            this.lock.readLock().lock();
            return super.getSupersetEntries(vertical);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public ArrayList<Entry<Vertical, Value>> getRestrictedSupersetEntries(Vertical vertical, Vertical exclusion) {
        try {
            this.lock.readLock().lock();
            return super.getRestrictedSupersetEntries(vertical, exclusion);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public Set<Vertical> keySet() {
        try {
            this.lock.readLock().lock();
            return super.keySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public Collection<Value> values() {
        try {
            this.lock.readLock().lock();
            return super.values();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public Set<Entry<Vertical, Value>> entrySet() {
        try {
            this.lock.readLock().lock();
            return super.entrySet();
        } finally {
            this.lock.readLock().unlock();
        }
    }

    @Override
    public void shrink(double factor, Comparator<Entry<Vertical, Value>> comparator, Predicate<Entry<Vertical, Value>> canRemove) {
        try {
            this.lock.readLock().lock();
            super.shrink(factor, comparator, canRemove);
        } finally {
            this.lock.readLock().unlock();
        }
    }
}
