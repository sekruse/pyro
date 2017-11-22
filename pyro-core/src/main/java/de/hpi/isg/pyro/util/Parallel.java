package de.hpi.isg.pyro.util;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Utilities for parallel execution.
 */
public class Parallel {

    public static <T, R> List<R> map(Collection<T> elements,
                                     Function<T, R> function,
                                     Executor executor,
                                     boolean isFailImmediately) {
        List<Future<R>> futures = new ArrayList<>(elements.size());
        for (T element : elements) {
            futures.add(executor.execute(() -> function.apply(element)));
        }
        ExecutorException executorException = null;
        List<R> results = new ArrayList<>(elements.size());
        for (Future<R> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException | ExecutionException e) {
                if (executorException == null) executorException = new ExecutorException();
                executorException.nestedExceptions.add(e);
                if (isFailImmediately) throw executorException;
            }
        }

        if (executorException != null) throw executorException;
        return results;
    }

    public static <T> void forEach(Collection<T> elements,
                                   Consumer<T> function,
                                   Executor executor,
                                   boolean isFailImmediately) {
        List<Future<?>> futures = new ArrayList<>(elements.size());
        for (T element : elements) {
            futures.add(executor.execute(() -> {
                function.accept(element);
                return null;
            }));
        }
        ExecutorException executorException = null;
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                if (executorException == null) executorException = new ExecutorException();
                executorException.nestedExceptions.add(e);
                if (isFailImmediately) throw executorException;
            }
        }

        if (executorException != null) throw executorException;
    }

    public static class Result<T> {

        private final T result;

        private final Exception exception;

        public Result(T result, Exception exception) {
            this.result = result;
            this.exception = exception;
        }

        public T getResult() {
            return result;
        }

        public Exception getException() {
            return exception;
        }
    }

    @FunctionalInterface
    public interface Executor {

        <T> Future<T> execute(Callable<T> t);

    }

    public static Executor threadLocalExecutor = new Executor() {
        @Override
        public <T> Future<T> execute(Callable<T> callable) {
            return new Future<T>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return false;
                }

                @Override
                public T get() throws InterruptedException, ExecutionException {
                    try {
                        return callable.call();
                    } catch (Exception e) {
                        throw new ExecutionException(e);
                    }
                }

                @Override
                public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                    throw new RuntimeException("Timed execution not supported.");
                }
            };
        }
    };

    public static <T> List<Tuple<T, Integer>> zipWithIndex(Collection<T> elements) {
        int index = 0;
        List<Tuple<T, Integer>> result = new ArrayList<>(elements.size());
        for (T element : elements) {
            result.add(new Tuple<>(element, index++));
        }
        return result;
    }

    public static IntList range(int to) {
        IntList range = new IntArrayList(to);
        for (int i = 0; i < to; i++) {
            range.add(to);
        }
        return range;
    }

    public static class ExecutorException extends RuntimeException {

        private final Collection<Exception> nestedExceptions = new LinkedList<>();

        public Collection<Exception> getNestedExceptions() {
            return this.nestedExceptions;
        }

        @Override
        public synchronized Throwable getCause() {
            return this.nestedExceptions.isEmpty() ? super.getCause() : this.nestedExceptions.iterator().next();
        }
    }

}
