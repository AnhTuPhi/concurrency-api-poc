package com.poc.concurrency.resilience;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Three states:
 *   CLOSED    — calls pass through. Track recent failures.
 *   OPEN      — calls short-circuit (fail fast). After cooldown, move to HALF_OPEN.
 *   HALF_OPEN — one probe call allowed. Success → CLOSED. Failure → OPEN again.
 */
public class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final int failureThreshold;
    private final long openCooldownMs;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicLong openedAtMs = new AtomicLong(0);

    public CircuitBreaker(int failureThreshold, long openCooldownMs) {
        this.failureThreshold = failureThreshold;
        this.openCooldownMs = openCooldownMs;
    }

    public <T> T call(Supplier<T> action) throws CircuitOpenException {
        State s = currentState();
        if (s == State.OPEN) {
            throw new CircuitOpenException("circuit OPEN — failing fast");
        }
        try {
            T result = action.get();
            onSuccess();
            return result;
        } catch (RuntimeException e) {
            onFailure();
            throw e;
        }
    }

    public State currentState() {
        State s = state.get();
        if (s == State.OPEN && System.currentTimeMillis() - openedAtMs.get() >= openCooldownMs) {
            if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                return State.HALF_OPEN;
            }
        }
        return state.get();
    }

    private void onSuccess() {
        consecutiveFailures.set(0);
        state.set(State.CLOSED);
    }

    private void onFailure() {
        int f = consecutiveFailures.incrementAndGet();
        if (state.get() == State.HALF_OPEN || f >= failureThreshold) {
            state.set(State.OPEN);
            openedAtMs.set(System.currentTimeMillis());
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(currentState().name(), consecutiveFailures.get(), failureThreshold, openCooldownMs);
    }

    public record Snapshot(String state, int consecutiveFailures, int threshold, long cooldownMs) {}

    public static class CircuitOpenException extends RuntimeException {
        public CircuitOpenException(String msg) { super(msg); }
    }
}
