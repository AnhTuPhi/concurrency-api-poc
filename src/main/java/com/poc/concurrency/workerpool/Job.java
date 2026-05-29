package com.poc.concurrency.workerpool;

import java.time.Instant;

public class Job {

    public enum State { QUEUED, RUNNING, DONE, FAILED }

    public final String id;
    public final int workMs;
    public volatile State state = State.QUEUED;
    public volatile Instant queuedAt = Instant.now();
    public volatile Instant startedAt;
    public volatile Instant finishedAt;
    public volatile String result;
    public volatile String error;

    public Job(String id, int workMs) {
        this.id = id;
        this.workMs = workMs;
    }
}
