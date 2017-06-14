package de.hpi.isg.pyro.algorithms;

import de.hpi.isg.pyro.model.*;

import java.util.function.Consumer;

/**
 * A discovery unit provides the interface and relevant data structures to perform a dependency discovery within some
 * scope.
 */
abstract public class AbstractDiscoveryUnit {

    /**
     * Sink for discovered partial FDs.
     */
    Consumer<PartialFD> fdConsumer;

    /**
     * Sink for discovered partial UCCs.
     */
    Consumer<PartialKey> uccConsumer;

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

}
