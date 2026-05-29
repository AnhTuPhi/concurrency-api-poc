package com.poc.concurrency.debounce;

import java.util.concurrent.*;

/**
 * Debounce: wait until quietMs has passed since the LAST call before firing.
 * Each new call cancels the previous pending fire.
 *
 * Use case: live-search input — fire only after the user stops typing.
 */
public class Debouncer<T> {

    private final long quietMs;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
            r -> Thread.ofVirtual().name("debouncer-").unstarted(r));
    private ScheduledFuture<?> pending;
    private T lastValue;

    public Debouncer(long quietMs) {
        this.quietMs = quietMs;
    }

    public synchronized void submit(T value, java.util.function.Consumer<T> action) {
        this.lastValue = value;
        if (pending != null) pending.cancel(false);
        pending = scheduler.schedule(() -> action.accept(lastValue), quietMs, TimeUnit.MILLISECONDS);
    }
}
