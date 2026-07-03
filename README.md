# Concurrency Patterns POC

Java 21 + Spring Boot. Four patterns in one runnable app, each with a live demo button.

## Documentation map

| Doc | What it answers |
|---|---|
| [ISSUE.md](ISSUE.md) | *Why* — the problem being solved: keeping an API alive under concurrent load and flaky dependencies, what we're protecting, scope & non-goals. |
| [TECHNICAL.md](TECHNICAL.md) | *How* — per POC: the hard problem, what it protects, solution shape, key tech **by responsibility**, how it solves each sub-problem, and tech debt to acknowledge. |
| [CONSISTENCY.md](CONSISTENCY.md) | *At scale* — what breaks when you run many pods / VMs behind a load balancer, and how to fix each pattern (Redis, broker, gateway, service mesh). |
| [`/`](src/main/resources/static/index.html) | Interactive demo — spam each pattern and watch it react. |
| [`/explain.html`](src/main/resources/static/explain.html) | Visual explainer — diagrams of the flow & key tech of each pattern and the scaling story. |

## The hard problem in each POC

Each pattern answers one way an unprotected API tips over (see [ISSUE.md](ISSUE.md)):

| POC | Failure mode it prevents | Core hard problem |
|---|---|---|
| **1. Rate limiting** | Overload / abusive clients | Per-client allow/deny at high concurrency without a global lock; two definitions of "too many" (burst-tolerant vs. strict cap). |
| **2. Debounce / throttle** | Redundant work | Collapse a flood of rapid events into the right amount of work without losing the latest intent. |
| **3. Worker pool + queue** | Unbounded buffering (OOM) | Absorb bursts with bounded resources and shed excess **fast** instead of buffering forever. |
| **4. Circuit breaker + retry** | Cascading failure | Stop hammering a dead downstream (breaker) and recover from transient blips without a synchronized stampede (retry + jitter). |

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
mvn spring-boot:run
```

- Interactive demo: <http://localhost:8081/>
- Visual explainer: <http://localhost:8081/explain.html>

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

The single biggest gap: **all state is in-process and node-local** — correct in one
JVM, wrong-by-default across replicas. That, plus the per-pattern debts, is covered
in depth in [TECHNICAL.md](TECHNICAL.md#the-one-debt-that-spans-all-four) and
[CONSISTENCY.md](CONSISTENCY.md). In short:

- **Distributed rate limiting**: in-memory state — the limit multiplies by replica count. Use a gateway or Redis (`INCR`+`EXPIRE`, or a Lua script for atomic sliding-window).
- **Persistent jobs**: queue is in-process. Real systems use a durable broker (Postgres `SELECT ... FOR UPDATE SKIP LOCKED`, RabbitMQ, SQS, Kafka) + KEDA autoscaling on lag.
- **Circuit breaker metrics**: real implementations use a rolling failure ratio (e.g. Resilience4j) — not consecutive count. Global tripping wants a service mesh (Envoy outlier detection).
- **Backpressure across the wire**: HTTP `503 Retry-After`, gRPC flow control, Reactive Streams (`Flow.Subscription.request(n)`).
- **Observability**: every pattern here should emit metrics (allowed/denied counter, queue depth gauge, breaker state gauge) and traces.
