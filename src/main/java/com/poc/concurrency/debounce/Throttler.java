package com.poc.concurrency.debounce;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Throttle: allow at most one execution per intervalMs. Extra calls are dropped.
 *
 * Use case: spam-clicked submit button — only first click per window fires.
 */
public class Throttler {

    private final long intervalMs;
    private final AtomicLong lastFireMs = new AtomicLong(0);

    public Throttler(long intervalMs) {
        this.intervalMs = intervalMs;
    }

    public boolean tryFire() {
        long now = System.currentTimeMillis();
        long last = lastFireMs.get();
        if (now - last >= intervalMs && lastFireMs.compareAndSet(last, now)) {
            return true;
        }
        return false;
    }
}
