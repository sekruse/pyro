package de.hpi.isg.pyro.core;

import de.hpi.isg.pyro.model.*;

import java.util.function.Consumer;

/**
 * A discovery unit provides the interface and relevant data structures to perform a dependency discovery within some
 * scope.
 */
public class DependencyConsumer {

    /**
     * Sink for discovered partial FDs.
     */
    protected Consumer<PartialFD> fdConsumer;

    /**
     * Sink for discovered partial UCCs.
     */
    protected Consumer<PartialKey> uccConsumer;

    /**
     * Registers a discovered partial FD.
     * @return the {@link PartialFD}
     */
    protected PartialFD registerFd(Vertical lhs, Column rhs, double error, double score) {
        PartialFD partialFD = new PartialFD(lhs, rhs, error, score);
        this.fdConsumer.accept(partialFD);
        return partialFD;
    }

    /**
     * Register a {@link PartialKey}.
     *
     * @return the {@link PartialKey}
     */
    protected PartialKey registerUcc(Vertical keyVertical, double error, double score) {
        PartialKey partialKey = new PartialKey(keyVertical, error, score);
        this.uccConsumer.accept(partialKey);
        return partialKey;
    }

    public void setFdConsumer(Consumer<PartialFD> fdConsumer) {
        this.fdConsumer = fdConsumer;
    }

    public void setUccConsumer(Consumer<PartialKey> uccConsumer) {
        this.uccConsumer = uccConsumer;
    }
}
