package com.poc.concurrency.ratelimit;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/rate-limit")
public class RateLimitController {

    private final TokenBucketRateLimiter bucket = new TokenBucketRateLimiter(5, 2.0);
    private final SlidingWindowRateLimiter window = new SlidingWindowRateLimiter(5, 1000);

    @PostMapping("/token-bucket")
    public ResponseEntity<?> tokenBucket(HttpServletRequest req) {
        String key = clientKey(req);
        if (bucket.tryAcquire(key)) {
            var snap = bucket.snapshot(key);
            return ResponseEntity.ok(Map.of("allowed", true, "snapshot", snap));
        }
        return ResponseEntity.status(429).body(Map.of("allowed", false, "snapshot", bucket.snapshot(key)));
    }

    @PostMapping("/sliding-window")
    public ResponseEntity<?> slidingWindow(HttpServletRequest req) {
        String key = clientKey(req);
        if (window.tryAcquire(key)) {
            return ResponseEntity.ok(Map.of("allowed", true, "snapshot", window.snapshot(key)));
        }
        return ResponseEntity.status(429).body(Map.of("allowed", false, "snapshot", window.snapshot(key)));
    }

    private String clientKey(HttpServletRequest req) {
        return req.getRemoteAddr();
    }
}
