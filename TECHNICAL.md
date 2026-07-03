# TECHNICAL.md — How each POC works, and what it costs

This document goes POC by POC. For each one:

- **The hard problem** — the concurrency problem underneath, not just the feature.
- **What we protect** — the resource or invariant at stake.
- **Solution shape** — the algorithm in one paragraph.
- **Key tech by responsibility** — which primitive does which job, and *why that one*.
- **How it solves each sub-problem** — the problem broken into parts, each mapped
  to a mechanism.
- **Tech debt to acknowledge** — what we knowingly skipped.

The single crosscutting debt — *all state is in-process and node-local* — is
called out here per pattern but analysed properly in
[CONSISTENCY.md](CONSISTENCY.md).

> Java 21, virtual threads enabled. Everything is hand-rolled (no Resilience4j,
> Bucket4j, Guava) so the mechanics are visible. Files referenced below live
> under `src/main/java/com/poc/concurrency/`.

---

## POC 1 — Rate Limiting

`ratelimit/TokenBucketRateLimiter.java`, `ratelimit/SlidingWindowRateLimiter.java`,
`ratelimit/RateLimitController.java`

### The hard problem
Decide **allow / deny** for every inbound request, *per client*, at high
concurrency, in microseconds, without a global lock that itself becomes the
bottleneck. The decision is a read-modify-write on shared counters — the classic
race. And "how many is too many" has two legitimate but different definitions:
*sustained rate with tolerated bursts* vs. *a hard ceiling in any window*.

### What we protect
Downstream capacity and CPU from a burst or an abusive client; **fairness** —
one caller's flood must not consume everyone else's share (hence per-key state).

### Solution shape
A `ConcurrentHashMap<key, state>` gives each client an isolated counter. Two
algorithms over that map:
- **Token bucket** — a bucket of `capacity` tokens refills at `refillPerSec`.
  Each request spends one token; empty bucket → deny. Refill is computed *lazily*
  on access from elapsed time, so there is no timer thread. Allows a burst up to
  `capacity`, then settles to the refill rate.
- **Sliding-window log** — keep the timestamps of accepted requests in the last
  `windowMs`; evict expired ones; allow only if fewer than `limit` remain. A
  strict cap with **no** burst beyond `limit`.

### Key tech by responsibility
| Responsibility | Mechanism | Why this one |
|---|---|---|
| Per-key isolation + concurrent map access | `ConcurrentHashMap` + `computeIfAbsent` | Lock-free lookups; each key gets its own state object atomically. |
| Mutual exclusion on one key's read-modify-write | `synchronized (bucket)` / `synchronized (deque)` | Lock is scoped to a **single key**, so contention is per-client, not global. |
| Refill / expiry math | `System.nanoTime()` (bucket), `System.currentTimeMillis()` (window) | `nanoTime` is monotonic — immune to wall-clock jumps/NTP steps; ms wall-clock is fine for the window log which only compares to itself. |
| No background threads | Lazy refill on access | A per-bucket timer would be O(clients) threads; computing refill from elapsed time is O(1) and thread-free. |

### How it solves each sub-problem
- **Burst tolerance** → token-bucket `capacity` (first N through instantly).
- **Sustained rate** → token-bucket `refillPerSec`.
- **Hard ceiling, no burst** → sliding-window `limit`/`windowMs`.
- **Thread-safety at scale** → lock striped per key, not one global lock.
- **Memory of "recent" without a timer** → lazy refill; window evicts on access.

### Tech debt to acknowledge
- **Node-local state.** In-memory per JVM → the effective limit multiplies by the
  number of replicas. See [CONSISTENCY.md](CONSISTENCY.md#poc-1).
- **Unbounded key map.** `buckets` / `log` never evict idle keys → memory grows
  with distinct clients (a leak, and a DoS vector). Needs TTL/LRU eviction.
- **Sliding-window memory.** The *log* variant stores one timestamp per accepted
  request → heavy under load. Production would use a counter-based sliding window
  or bucketed approximation.
- **Client key = `req.getRemoteAddr()`.** Behind a proxy/LB this is the LB's IP →
  everyone shares one bucket. Must read a trusted `X-Forwarded-For`.
- **No `Retry-After` header** on the 429; clients can't back off intelligently.

---

## POC 2 — Debounce & Throttle

`debounce/Debouncer.java`, `debounce/Throttler.java`,
`debounce/DebounceThrottleController.java`

### The hard problem
Collapse a **flood of rapid events** into the right amount of work without losing
the caller's latest intent. Two shapes: *debounce* = "do it once, after things go
quiet" (trailing edge); *throttle* = "do it at most once per interval, drop the
rest" (leading edge). Both are stateful over time and hit concurrently.

### What we protect
An expensive action (search backend query, autosave, webhook fan-out, an
idempotent-but-costly write) from being run for every keystroke / every click.
Also the **"last write wins"** semantic for debounce.

### Solution shape
- **Debounce** — a single-slot scheduled task. Each `submit` records the newest
  value, **cancels** the previously scheduled fire, and schedules a new one
  `quietMs` out. Only a submit with no successor within `quietMs` actually fires,
  and it fires with the latest value.
- **Throttle** — a timestamp gate. `tryFire` reads `lastFire`, and only if
  `now - lastFire >= interval` does it try to `compareAndSet` the timestamp. The
  single winner of the CAS fires; concurrent callers lose and are dropped.

### Key tech by responsibility
| Responsibility | Mechanism | Why this one |
|---|---|---|
| Deferred, cancellable firing | `ScheduledExecutorService` (single virtual thread) | Gives an ordered timer we can cancel/reschedule; one thread is enough for one slot. |
| Coherent "cancel old + record latest + schedule new" | `synchronized submit` + `ScheduledFuture.cancel(false)` | The three steps must be atomic w.r.t. each other or two submits race and fire twice. `cancel(false)` = don't interrupt an already-running fire. |
| Exactly-once-per-window gate, lock-free | `AtomicLong` + `compareAndSet` | The window check + claim is a single CAS; only one thread can advance the timestamp, so exactly one fires. No lock, no blocking. |

### How it solves each sub-problem
- **Fire once after quiet** → cancel-and-reschedule single slot (debounce).
- **Keep the latest value** → `lastValue` written under the same lock, read at fire.
- **Cap firing rate, leading edge** → timestamp gate (throttle).
- **No lock on the hot path for throttle** → CAS instead of `synchronized`.
- **No busy-wait / no timer-per-event** → one scheduler, tasks cancelled cheaply.

### Tech debt to acknowledge
- **Not keyed.** One shared `Debouncer`/`Throttler` for the whole controller →
  *all* users collapse into one stream. Real use needs per-user/per-entity keys
  (`Map<key, Debouncer>`), which reintroduces the eviction problem from POC 1.
- **Wrong tier (for debounce).** Debouncing keystrokes *server-side* still pays a
  network round-trip per keystroke — the thing debounce exists to avoid. Debounce
  belongs on the **client**; it lives here only to make the mechanics observable.
- **Throttle has no trailing fire.** The last dropped event in a window is lost.
  Some use cases want "leading + trailing".
- **Lifecycle.** The `ScheduledExecutorService` is never shut down (no
  `@PreDestroy`) → thread leak on context reload.
- **Node-local.** Each replica debounces/throttles independently — see
  [CONSISTENCY.md](CONSISTENCY.md#poc-2).

---

## POC 3 — Worker Pool / Job Queue (backpressure)

`workerpool/WorkerPool.java`, `workerpool/Job.java`, `workerpool/WorkerPoolController.java`

### The hard problem
Absorb **bursty submissions with bounded resources** and, when the burst exceeds
capacity, **shed load fast** instead of buffering forever. Unbounded queueing
converts a fast, honest failure ("busy, try later") into a slow catastrophic one
(latency grows without bound, then OOM). The producer needs an immediate,
truthful signal that we are full.

### What we protect
Heap (bounded buffer), the service from **unbounded concurrency** (only N jobs run
at once), and the producer's ability to react (fast `503` = backpressure signal,
not a hang).

### Solution shape
An `ArrayBlockingQueue(capacity)` is the buffer *and* the backpressure boundary.
`N` workers on **virtual threads** each loop: `poll` a job, run it, repeat.
Submission uses `offer()` (non-blocking): if the queue is full it returns `false`
immediately → the controller answers `503 "queue full — backpressure"`. So the
system accepts exactly `N running + capacity queued`; everything beyond that is
rejected now, not later.

### Key tech by responsibility
| Responsibility | Mechanism | Why this one |
|---|---|---|
| Bounded buffer = the backpressure line | `ArrayBlockingQueue(cap)` | Fixed capacity is the explicit limit; "full" is a first-class, observable state. |
| Reject-now instead of block-the-caller | `queue.offer()` (non-blocking) | `put()` would block the request thread (hidden backpressure → threads pile up). `offer()` fails fast so we can return `503`. |
| Cheap workers that block on I/O | Virtual threads (`Thread.ofVirtual`, `newThreadPerTaskExecutor`) | Workers `sleep`/do blocking I/O; virtual threads make blocking cheap — no platform-thread starvation, thousands are affordable. |
| Bounded live concurrency | Fixed `WORKERS` count of loops | The real throttle on the *downstream* is N, independent of queue size. |
| Graceful shutdown | `volatile running` + `poll(timeout)` + `shutdownNow()` | Workers wake at least every 200ms to re-check `running`, so they exit promptly instead of blocking forever on an empty queue. |
| Status without blocking writers | `ConcurrentHashMap<id, Job>` + `volatile` fields on `Job` | Lets `/stats` and lookups read job state concurrently with workers mutating it. |

### How it solves each sub-problem
- **Absorb bursts** → the queue (up to `capacity`).
- **Cap concurrency on the downstream** → exactly `WORKERS` running.
- **Shed excess fast** → `offer()==false` → `503` immediately.
- **Don't hang the caller** → non-blocking submit, no `put()`.
- **Stop cleanly** → running flag + timed poll + `shutdownNow`.
- **Observe pressure** → `/stats` exposes `queueDepth/capacity`, running, done.

### Tech debt to acknowledge
- **Not durable.** The queue is in-process → a crash/redeploy drops all queued and
  in-flight jobs. Real async work needs a durable broker (Postgres
  `SKIP LOCKED`, RabbitMQ, SQS, Kafka).
- **`jobs` map grows forever.** Completed jobs are never evicted → memory leak;
  needs TTL/size cap. `stats()` is O(n) over that growing map.
- **No retry / DLQ / priority.** A `FAILED` job is terminal; no dead-letter, no
  reordering.
- **Backpressure ends at the socket.** We return `503` but no `Retry-After`; the
  client is trusted to back off.
- **Node-local queue.** Backpressure is per-replica; a rejected client retried to
  another pod may be accepted — see [CONSISTENCY.md](CONSISTENCY.md#poc-3).

---

## POC 4 — Circuit Breaker & Retry with Backoff + Jitter

`resilience/CircuitBreaker.java`, `resilience/RetryWithBackoff.java`,
`resilience/ResilienceController.java`, `demo/FlakyService.java`

### The hard problem
When a downstream is failing or slow, **stop making it worse and stop letting it
take you down with it.** Two complementary jobs:
1. *Recognise* sustained failure and **stop calling** for a while (breaker), so
   request threads don't all block on a dead dependency and cascade into a total
   outage.
2. *Recover* from **transient** blips without a **synchronized stampede** — many
   clients retrying at the same instant is a self-inflicted DDoS on a service
   that's already struggling.
These pull in opposite directions (retry = call more; breaker = call less), so
they must be composed deliberately.

### What we protect
The downstream from a thundering herd during an outage; our **request threads**
from piling up on a dependency that will never answer; end-to-end latency (fail
fast beats hanging); and successful completion of calls that fail only transiently.

### Solution shape
- **Circuit breaker** — a three-state machine over atomics.
  `CLOSED` (pass through, count consecutive failures) → at `failureThreshold`
  flip to `OPEN` (reject instantly, don't touch downstream) → after
  `openCooldownMs` lazily become `HALF_OPEN` on the next read → allow **one**
  probe: success → `CLOSED`, failure → `OPEN` again.
- **Retry** — a bounded loop: up to `maxAttempts`, sleeping
  `delay(n) = min(maxDelay, base·2ⁿ) + random(0..jitter)` between tries.
  Exponential growth backs off fast; the cap bounds it; **jitter decorrelates**
  clients so they don't retry in lockstep.

### Key tech by responsibility
| Responsibility | Mechanism | Why this one |
|---|---|---|
| Lock-free state transitions | `AtomicReference<State>` + `compareAndSet` | State flips are contended across many request threads; CAS avoids a lock on the hot path. |
| Exactly one probe in HALF_OPEN | `compareAndSet(OPEN, HALF_OPEN)` on read | Only the single thread that wins the CAS gets to probe; the rest still see OPEN and fail fast. |
| Failure accounting / cooldown clock | `AtomicInteger consecutiveFailures`, `AtomicLong openedAtMs` | Coordinate the threshold and the time-based reopen without a lock. |
| Lazy OPEN→HALF_OPEN | Cooldown check inside `currentState()` | No scheduler needed; the transition happens on the next call after the cooldown elapses. |
| Bounded, decorrelated retry timing | `base·2ⁿ` capped by `min(maxDelay, …)` **+** `ThreadLocalRandom` jitter | Exponential = fast backoff; cap = no runaway delay; jitter = no synchronized stampede. |

### How it solves each sub-problem
- **Stop hammering a dead downstream** → `OPEN` short-circuits (throws without calling).
- **Don't stay blind forever** → time-based cooldown → `HALF_OPEN`.
- **Probe safely** → single-CAS gate admits exactly one trial call.
- **Recover from transient errors** → bounded retry loop.
- **Bounded worst-case delay** → `min(maxDelay, base·2ⁿ)`.
- **No thundering herd** → additive jitter spreads retries in time.

### Tech debt to acknowledge
- **Consecutive-count breaker, not a rolling ratio.** One success resets the
  counter, so an endpoint failing 50% of the time may never trip. Production
  (e.g. Resilience4j) uses a rolling failure-rate/slow-call window.
- **Retry blocks the calling thread** (`Thread.sleep`). Fine on virtual threads;
  on platform threads it burns a pooled thread. No global **retry budget**, so a
  broad outage means every request retries `maxAttempts×` → amplified load.
- **No retryable-vs-fatal classification.** It retries *any* `RuntimeException`,
  including ones that will never succeed (400, auth failure).
- **Breaker + retry not composed here.** They're demoed on separate endpoints;
  naively stacking retry *inside* a breaker can trip it faster or retry through it
  — composition order matters.
- **Per-instance view.** Each replica learns "downstream is down" independently →
  slower collective reaction — see [CONSISTENCY.md](CONSISTENCY.md#poc-4).

---

## The one debt that spans all four

Every pattern above keeps its state in a field of a singleton bean — a
`ConcurrentHashMap`, an `AtomicLong`, an `ArrayBlockingQueue`. That is exactly
what makes the POC legible and dependency-free, and exactly what breaks the moment
you run **more than one copy** of the process. Rate limits multiply by replica
count, debouncers fire once per pod, backpressure is per-pod, and each breaker
learns about an outage on its own. What "correct" even means changes when you
scale horizontally. That is the subject of [CONSISTENCY.md](CONSISTENCY.md).
