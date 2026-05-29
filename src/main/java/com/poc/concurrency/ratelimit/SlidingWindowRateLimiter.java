package com.poc.concurrency.ratelimit;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sliding window log: keep timestamps of requests in the last `windowMs`.
 * If count < limit, accept; else reject.
 *
 * Stricter than token bucket — no burst beyond `limit` per window.
 */
public class SlidingWindowRateLimiter {

    private final int limit;
    private final long windowMs;
    private final Map<String, Deque<Long>> log = new ConcurrentHashMap<>();

    public SlidingWindowRateLimiter(int limit, long windowMs) {
        this.limit = limit;
        this.windowMs = windowMs;
    }

    public boolean tryAcquire(String key) {
        Deque<Long> q = log.computeIfAbsent(key, k -> new ArrayDeque<>());
        long now = System.currentTimeMillis();
        long cutoff = now - windowMs;
        synchronized (q) {
            while (!q.isEmpty() && q.peekFirst() < cutoff) q.pollFirst();
            if (q.size() < limit) {
                q.addLast(now);
                return true;
            }
            return false;
        }
    }

    public Snapshot snapshot(String key) {
        Deque<Long> q = log.get(key);
        if (q == null) return new Snapshot(limit, 0, windowMs);
        synchronized (q) {
            long cutoff = System.currentTimeMillis() - windowMs;
            while (!q.isEmpty() && q.peekFirst() < cutoff) q.pollFirst();
            return new Snapshot(limit, q.size(), windowMs);
        }
    }

    public record Snapshot(int limit, int used, long windowMs) {}
}
