# Concurrency Patterns POC

Java 21 + Spring Boot. Four patterns in one runnable app, each with a live demo button.

| Pattern | Where | Idea |
|---|---|---|
| **Token bucket** rate limiter | `ratelimit/TokenBucketRateLimiter.java` | Refill steadily, allow bursts up to capacity |
| **Sliding window** rate limiter | `ratelimit/SlidingWindowRateLimiter.java` | Count requests in the last N ms — strict cap |
| **Debounce** | `debounce/Debouncer.java` | Fire once after caller goes quiet for X ms |
| **Throttle** | `debounce/Throttler.java` | At most one fire per X ms |
| **Worker pool / job queue** | `workerpool/WorkerPool.java` | Bounded queue + N virtual-thread workers; full queue = backpressure |
| **Circuit breaker** | `resilience/CircuitBreaker.java` | CLOSED → OPEN (fail fast) → HALF_OPEN (probe) |
| **Retry + backoff + jitter** | `resilience/RetryWithBackoff.java` | `delay = min(max, base·2ⁿ) + jitter` |

## Run

```bash
cd "/Users/pat/Documents/Code/Github Personal/concurrency-poc"
./mvnw spring-boot:run            # or: mvn spring-boot:run
```

Open <http://localhost:8081>.

Requirements: JDK 21 + Maven. (No `mvnw` is checked in — use system Maven, or run `mvn -N io.takari:maven:wrapper` first.)

## What each demo shows

### 1. Rate limiting
Spam 20 requests over 1 second.
- **Token bucket** (capacity 5, refill 2/sec): first 5 ALLOWED (burst), then ~2/sec.
- **Sliding window** (5 / 1 sec): only 5 ALLOWED inside any 1-second window, the rest 429.

### 2. Debounce & throttle
- **Debounce**: type fast — the server only logs *one* fire, 400ms after you stop.
- **Throttle**: spam-click — only the first click per 1s fires; the rest are dropped.

### 3. Worker pool / job queue
3 workers + queue capacity 10. Submit 30 jobs at once → 13 accepted (3 in flight + 10 queued), 17 rejected with `503` — that's **backpressure at the application layer**. Stats refresh every 500ms.

### 4. Circuit breaker & retry
Flaky downstream fails X% of calls (slider 0–100).
- **Raw**: pass/fail directly mirrors flaky rate.
- **Circuit breaker**: after 3 consecutive fails → state flips OPEN, calls fail fast for 5s without touching downstream → one HALF_OPEN probe → CLOSED or OPEN again.
- **Retry**: up to 4 attempts, exponentially-growing delay (100ms, 200ms, 400ms, 800ms) + jitter. You'll see the per-attempt log.

## API surface

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/rate-limit/token-bucket` | Try one token |
| `POST` | `/api/rate-limit/sliding-window` | Try one slot |
| `POST` | `/api/debounce-throttle/debounce?value=…` | Submit one value to the debouncer |
| `POST` | `/api/debounce-throttle/throttle` | One throttled fire attempt |
| `POST` | `/api/workers/jobs?workMs=1500` | Enqueue a job |
| `GET`  | `/api/workers/stats` | Queue depth + counts |
| `POST` | `/api/resilience/raw` | Hit flaky directly |
| `POST` | `/api/resilience/circuit-breaker` | Hit through breaker |
| `POST` | `/api/resilience/retry` | Hit through retry+backoff |
| `POST` | `/api/resilience/flaky?failureRatePct=70` | Tune flakiness |

## Notes on production-readiness (what this POC skips)

- **Distributed rate limiting**: in-memory state — won't work across multiple app instances. Use Redis (e.g. `INCR` + `EXPIRE`, or a Lua script for atomic sliding-window).
- **Persistent jobs**: queue is in-process. Real systems use a durable broker (Postgres `SELECT ... FOR UPDATE SKIP LOCKED`, RabbitMQ, SQS, Kafka).
- **Circuit breaker metrics**: real implementations use a rolling failure ratio (e.g. Resilience4j) — not consecutive count.
- **Backpressure across the wire**: HTTP `503 Retry-After`, gRPC flow control, Reactive Streams (`Flow.Subscription.request(n)`).
- **Observability**: every pattern here should emit metrics (allowed/denied counter, queue depth gauge, breaker state gauge) and traces.
