package com.poc.concurrency.demo;

import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pretends to be a flaky downstream — fails `failureRate` percent of the time.
 * Tunable so the UI can flip between "healthy" and "broken" modes.
 */
@Component
public class FlakyService {

    private volatile int failureRatePct = 70;
    private final AtomicInteger calls = new AtomicInteger(0);

    public String call() {
        calls.incrementAndGet();
        if (ThreadLocalRandom.current().nextInt(100) < failureRatePct) {
            throw new RuntimeException("downstream failed (simulated)");
        }
        return "ok-" + calls.get();
    }

    public void setFailureRatePct(int pct) {
        this.failureRatePct = Math.max(0, Math.min(100, pct));
    }

    public int getFailureRatePct() {
        return failureRatePct;
    }

    public int getCalls() {
        return calls.get();
    }
}
