package com.poc.concurrency.resilience;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

/**
 * Retry with exponential backoff + jitter.
 *
 *   delay(n) = min(maxDelay, base * 2^n) + random(0, jitter)
 *
 * Jitter is important — without it, all clients retry at the same instant and stampede the server.
 */
public class RetryWithBackoff {

    private final int maxAttempts;
    private final long baseMs;
    private final long maxDelayMs;
    private final long jitterMs;

    public RetryWithBackoff(int maxAttempts, long baseMs, long maxDelayMs, long jitterMs) {
        this.maxAttempts = maxAttempts;
        this.baseMs = baseMs;
        this.maxDelayMs = maxDelayMs;
        this.jitterMs = jitterMs;
    }

    public <T> Result<T> execute(Supplier<T> action) {
        List<Attempt> attempts = new ArrayList<>();
        RuntimeException last = null;
        for (int n = 0; n < maxAttempts; n++) {
            long startedAt = System.currentTimeMillis();
            try {
                T value = action.get();
                attempts.add(new Attempt(n + 1, startedAt, true, null));
                return new Result<>(value, true, attempts);
            } catch (RuntimeException e) {
                last = e;
                attempts.add(new Attempt(n + 1, startedAt, false, e.getMessage()));
                if (n == maxAttempts - 1) break;
                long delay = Math.min(maxDelayMs, baseMs * (1L << n))
                           + ThreadLocalRandom.current().nextLong(jitterMs + 1);
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        throw new RetryExhaustedException("retries exhausted", last, attempts);
    }

    public record Attempt(int n, long startedAtMs, boolean ok, String error) {}
    public record Result<T>(T value, boolean ok, List<Attempt> attempts) {}

    public static class RetryExhaustedException extends RuntimeException {
        public final List<Attempt> attempts;
        public RetryExhaustedException(String msg, Throwable cause, List<Attempt> attempts) {
            super(msg, cause);
            this.attempts = attempts;
        }
    }
}
