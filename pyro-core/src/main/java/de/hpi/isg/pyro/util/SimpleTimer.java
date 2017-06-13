package de.hpi.isg.pyro.util;

import java.util.function.LongConsumer;

/**
 * Simple timer utility.
 */
public class SimpleTimer {

    private long startMillis;

    {
        this.start();
    }

    public void start() {
        this.startMillis = System.currentTimeMillis();
    }

    public long takeMillis() {
        return System.currentTimeMillis() - this.startMillis;
    }

    public void takeMillis(LongConsumer consumer) {
        consumer.accept(this.takeMillis());
    }

}
