package de.hpi.isg.pyro.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.LinkedList;
import java.util.List;

/**
 * Periodically watches the memory usage of the JVM and notifies listener when a certain usage threshed is surpassed.
 */
public final class MemoryWatchdog implements Runnable {

    private boolean keepRunning = true;

    private final long sleepMillis;

    private final double maxUsageRatio;

    private final MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

    private final Thread thread;

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final List<Runnable> listeners = new LinkedList<>();

    public static MemoryWatchdog start(double maxUsageRatio, long sleepMillis) {
        MemoryWatchdog memoryWatchdog = new MemoryWatchdog(maxUsageRatio, sleepMillis);
        memoryWatchdog.thread.start();
        return memoryWatchdog;
    }

    private MemoryWatchdog(double maxUsageRatio, long sleepMillis) {
        this.maxUsageRatio = maxUsageRatio;
        this.sleepMillis = sleepMillis;
        this.thread = new Thread(this, "memory-watchdog");
    }

    public void addListener(Runnable listener) {
        this.listeners.add(listener);
    }

    @Override
    public void run() {
        while (this.keepRunning) {
            MemoryUsage heapMemoryUsage = this.memoryMXBean.getHeapMemoryUsage();
            if (heapMemoryUsage.getUsed() > this.maxUsageRatio * heapMemoryUsage.getMax()) {
                this.logger.info("Critical memory consumption: {} MB / {} MB",
                        String.format("%,d", heapMemoryUsage.getUsed() / 1024 / 1024),
                        String.format("%,d", heapMemoryUsage.getMax() / 1024 / 1024)
                );
                for (Runnable listener : listeners) {
                    try {
                        listener.run();
                    } catch (Exception e) {
                        this.logger.error("Memory handling failed.", e);
                    }
                }
            }
            try {
                Thread.sleep(this.sleepMillis);
            } catch (InterruptedException e) {
                // Ignore.
            }
        }
        this.logger.info("Terminating...");
    }

    public void stop() {
        this.keepRunning = false;
        this.thread.interrupt();
    }
}
