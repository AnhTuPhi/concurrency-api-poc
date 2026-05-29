package com.poc.concurrency.resilience;

import com.poc.concurrency.demo.FlakyService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/resilience")
public class ResilienceController {

    private final FlakyService flaky;
    private final CircuitBreaker breaker = new CircuitBreaker(3, 5000);
    private final RetryWithBackoff retry = new RetryWithBackoff(4, 100, 2000, 50);

    public ResilienceController(FlakyService flaky) {
        this.flaky = flaky;
    }

    @PostMapping("/raw")
    public ResponseEntity<?> raw() {
        try {
            return ResponseEntity.ok(Map.of("ok", true, "value", flaky.call()));
        } catch (RuntimeException e) {
            return ResponseEntity.status(502).body(Map.of("ok", false, "error", e.getMessage()));
        }
    }

    @PostMapping("/circuit-breaker")
    public ResponseEntity<?> withBreaker() {
        try {
            String v = breaker.call(flaky::call);
            return ResponseEntity.ok(Map.of("ok", true, "value", v, "breaker", breaker.snapshot()));
        } catch (CircuitBreaker.CircuitOpenException e) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "shortCircuited", true, "breaker", breaker.snapshot()));
        } catch (RuntimeException e) {
            return ResponseEntity.status(502).body(Map.of("ok", false, "error", e.getMessage(), "breaker", breaker.snapshot()));
        }
    }

    @PostMapping("/retry")
    public ResponseEntity<?> withRetry() {
        try {
            var result = retry.execute(flaky::call);
            return ResponseEntity.ok(Map.of("ok", true, "value", result.value(), "attempts", result.attempts()));
        } catch (RetryWithBackoff.RetryExhaustedException e) {
            return ResponseEntity.status(502).body(Map.of("ok", false, "error", e.getMessage(), "attempts", e.attempts));
        }
    }

    @GetMapping("/breaker-state")
    public CircuitBreaker.Snapshot state() {
        return breaker.snapshot();
    }

    @PostMapping("/flaky")
    public Map<String, Object> setFlaky(@RequestParam int failureRatePct) {
        flaky.setFailureRatePct(failureRatePct);
        return Map.of("failureRatePct", flaky.getFailureRatePct());
    }

    @GetMapping("/flaky")
    public Map<String, Object> getFlaky() {
        return Map.of("failureRatePct", flaky.getFailureRatePct(), "calls", flaky.getCalls());
    }
}
