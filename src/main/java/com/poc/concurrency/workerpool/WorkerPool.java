package com.poc.concurrency.workerpool;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Bounded job queue + N workers running on virtual threads.
 *
 * Bounded queue is itself a form of backpressure: if it's full, the producer
 * is told to back off (returns false from submit) instead of buffering forever.
 */
@Component
public class WorkerPool {

    private static final int WORKERS = 3;
    private static final int QUEUE_CAPACITY = 10;

    private final BlockingQueue<Job> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final Map<String, Job> jobs = new ConcurrentHashMap<>();
    private final ExecutorService workers = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("worker-", 0).factory());
    private volatile boolean running = true;

    @PostConstruct
    public void start() {
        for (int i = 0; i < WORKERS; i++) {
            workers.submit(this::loop);
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        workers.shutdownNow();
    }

    /** Returns false if the queue is full — that's backpressure. */
    public boolean submit(Job job) {
        if (!queue.offer(job)) return false;
        jobs.put(job.id, job);
        return true;
    }

    public Job get(String id) { return jobs.get(id); }

    public java.util.Collection<Job> all() { return jobs.values(); }

    public Stats stats() {
        long queued = jobs.values().stream().filter(j -> j.state == Job.State.QUEUED).count();
        long running = jobs.values().stream().filter(j -> j.state == Job.State.RUNNING).count();
        long done = jobs.values().stream().filter(j -> j.state == Job.State.DONE).count();
        return new Stats(WORKERS, QUEUE_CAPACITY, queue.size(), queued, running, done);
    }

    private void loop() {
        while (running) {
            try {
                Job job = queue.poll(200, TimeUnit.MILLISECONDS);
                if (job == null) continue;
                runOne(job);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void runOne(Job job) {
        job.state = Job.State.RUNNING;
        job.startedAt = Instant.now();
        try {
            Thread.sleep(job.workMs);
            job.result = "processed in " + job.workMs + "ms by " + Thread.currentThread().getName();
            job.state = Job.State.DONE;
        } catch (InterruptedException e) {
            job.error = "interrupted";
            job.state = Job.State.FAILED;
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            job.error = e.getMessage();
            job.state = Job.State.FAILED;
        } finally {
            job.finishedAt = Instant.now();
        }
    }

    public record Stats(int workers, int queueCapacity, int queueDepth, long queued, long running, long done) {}
}
