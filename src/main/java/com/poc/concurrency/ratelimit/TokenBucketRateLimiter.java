package com.poc.concurrency.ratelimit;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Token bucket: refill at a steady rate, allow bursts up to capacity.
 * Each key (e.g., IP) has its own bucket.
 *
 * Capacity = max burst. RefillPerSec = sustained rate.
 */
public class TokenBucketRateLimiter {

    private final long capacity;
    private final double refillPerSec;
    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(long capacity, double refillPerSec) {
        this.capacity = capacity;
        this.refillPerSec = refillPerSec;
    }

    public boolean tryAcquire(String key) {
        Bucket b = buckets.computeIfAbsent(key, k -> new Bucket(capacity, System.nanoTime()));
        synchronized (b) {
            long now = System.nanoTime();
            double elapsedSec = (now - b.lastRefillNanos) / 1_000_000_000.0;
            double refill = elapsedSec * refillPerSec;
            b.tokens = Math.min(capacity, b.tokens + refill);
            b.lastRefillNanos = now;
            if (b.tokens >= 1.0) {
                b.tokens -= 1.0;
                return true;
            }
            return false;
        }
    }

    public Snapshot snapshot(String key) {
        Bucket b = buckets.get(key);
        if (b == null) return new Snapshot(capacity, capacity, refillPerSec);
        synchronized (b) {
            return new Snapshot(capacity, (long) b.tokens, refillPerSec);
        }
    }

    private static final class Bucket {
        double tokens;
        long lastRefillNanos;
        Bucket(long initial, long now) {
            this.tokens = initial;
            this.lastRefillNanos = now;
        }
    }

    public record Snapshot(long capacity, long available, double refillPerSec) {}
}
