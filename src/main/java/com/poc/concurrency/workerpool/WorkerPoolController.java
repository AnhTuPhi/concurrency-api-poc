package com.poc.concurrency.workerpool;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/workers")
public class WorkerPoolController {

    private final WorkerPool pool;

    public WorkerPoolController(WorkerPool pool) {
        this.pool = pool;
    }

    @PostMapping("/jobs")
    public ResponseEntity<?> submit(@RequestParam(defaultValue = "1500") int workMs) {
        Job job = new Job(UUID.randomUUID().toString().substring(0, 8), workMs);
        if (!pool.submit(job)) {
            return ResponseEntity.status(503).body(Map.of("accepted", false, "reason", "queue full — backpressure"));
        }
        return ResponseEntity.ok(Map.of("accepted", true, "jobId", job.id));
    }

    @GetMapping("/stats")
    public WorkerPool.Stats stats() {
        return pool.stats();
    }

    @GetMapping("/jobs")
    public java.util.Collection<Job> jobs() {
        return pool.all();
    }
}
