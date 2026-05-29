package com.poc.concurrency.debounce;

import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Demo is server-side for clarity, but in real apps debounce/throttle usually live
 * on the client (browser) — same algorithm, different runtime.
 */
@RestController
@RequestMapping("/api/debounce-throttle")
public class DebounceThrottleController {

    private final Debouncer<String> debouncer = new Debouncer<>(400);
    private final Throttler throttler = new Throttler(1000);

    private final List<String> debouncedFires = new CopyOnWriteArrayList<>();
    private final List<String> throttledFires = new CopyOnWriteArrayList<>();

    @PostMapping("/debounce")
    public Map<String, Object> debounce(@RequestParam String value) {
        debouncer.submit(value, v -> debouncedFires.add(Instant.now() + " :: " + v));
        return Map.of("submitted", value, "fires", new ArrayList<>(debouncedFires));
    }

    @PostMapping("/throttle")
    public Map<String, Object> throttle() {
        boolean fired = throttler.tryFire();
        if (fired) throttledFires.add(Instant.now().toString());
        return Map.of("fired", fired, "fires", new ArrayList<>(throttledFires));
    }

    @PostMapping("/reset")
    public Map<String, Object> reset() {
        debouncedFires.clear();
        throttledFires.clear();
        return Map.of("ok", true);
    }
}
