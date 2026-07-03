# CONSISTENCY.md — What breaks when you scale to many pods / VMs

Everything in this POC is **correct in one JVM** and **wrong-by-default in many.**
Every pattern holds its state in process memory (`ConcurrentHashMap`,
`AtomicLong`, `ArrayBlockingQueue`). Run the app as `N` replicas behind a load
balancer — the normal k8s Deployment or a VM autoscaling group — and you now have
`N` independent copies of that state, each seeing only the slice of traffic the LB
happened to route to it.

This document explains, per pattern, exactly how it drifts, then the mechanisms to
fix it, then the k8s / VM specifics (autoscaling, clock skew, rolling deploys,
pod identity) that make it subtle.

```
                         ┌───────── Load Balancer / Service ─────────┐
                         │        (round-robin, no affinity)         │
                         └───┬──────────────┬──────────────┬─────────┘
                             ▼              ▼              ▼
                        ┌─────────┐    ┌─────────┐    ┌─────────┐
                        │  Pod A  │    │  Pod B  │    │  Pod C  │
                        │ limiter │    │ limiter │    │ limiter │   ← 3 private copies
                        │ breaker │    │ breaker │    │ breaker │      of every counter
                        │ queue   │    │ queue   │    │ queue   │
                        └─────────┘    └─────────┘    └─────────┘
```

The mental model: **the algorithm assumes it sees every event. Behind an LB, each
replica sees ~1/N of them.** Any pattern whose correctness depends on a global
count is wrong until the count is shared.

---

## The core question: where should the state live?

There are four honest answers. Most systems mix them.

| Option | State lives in | Good for | Cost |
|---|---|---|---|
| **A. Node-local (as-is)** | Each pod's heap | Things that only protect *that pod's* own resources (its thread pool, its heap). | Free, fast, but no global guarantee. |
| **B. Shared store** | Redis / DB, called by every pod | A single global count/decision (rate limit, throttle). | A network hop per decision; the store is a new dependency & SPOF. |
| **C. Externalized substrate** | A broker / mesh does the job | Queues → Kafka/SQS; breaker/retry → service mesh sidecar. | Operational weight, but the concern leaves your code. |
| **D. Edge / gateway** | API gateway before the pods | Rate limiting, coarse throttling — decided once, before fan-out. | Another tier; less per-tenant nuance. |

The rest of this doc picks among these per pattern.

---

<a name="poc-1"></a>
## POC 1 — Rate limiting across replicas

### How it drifts
- **The limit multiplies by N.** "5 req/sec" configured per pod becomes `5 × N`
  globally. Scale from 3 → 10 pods (HPA) and your rate limit silently **triples**
  without anyone changing a config.
- **Inconsistent decisions.** The same client, round-robined across pods, is
  counted separately on each → it can exceed any single pod's limit while no pod
  sees a violation.
- **`remoteAddr` is the LB.** Behind a proxy every request looks like it comes
  from the LB's IP → one shared bucket for the world (see TECHNICAL.md debt).

### Fixes (best first)
1. **Decide at the gateway (Option D).** Rate limit in the API gateway / Envoy /
   NGINX *before* traffic fans out to pods. One vantage point sees all traffic;
   the count is global by construction. Best default for coarse per-client limits.
2. **Centralize in Redis (Option B).** Keep the algorithm but move the counter to
   Redis, executed as an **atomic Lua script** so check-and-decrement is one
   round trip and race-free:
   - *Token bucket*: store `{tokens, lastRefill}` per key; Lua refills from
     elapsed time and decrements — the exact algorithm in
     `TokenBucketRateLimiter`, just with shared state.
   - *Sliding window*: a Redis **sorted set** of timestamps (`ZADD` /
     `ZREMRANGEBYSCORE` / `ZCARD`), or the cheaper fixed-window `INCR`+`EXPIRE`.
   Use **Redis `TIME`** as the clock so all pods share one clock (see skew below).
3. **Local approximation (Option A+).** Give each pod `limit/N` and accept drift.
   Cheap and store-free, but `N` changes under autoscaling and traffic isn't even
   across pods → both under- and over-counting. Only for soft limits.

**Trade-off:** centralizing adds a Redis hop (~0.2–1ms) and makes Redis a
dependency on the request path. For a hard financial limit that's the right price;
for a soft "be nice" limit, the gateway or an approximation is enough.

---

<a name="poc-2"></a>
## POC 2 — Debounce / throttle across replicas

### How it drifts
- **Debounce fires once *per pod*.** Keystrokes for one user spread across 3 pods
  → up to 3 trailing fires instead of 1. The whole point ("collapse to one") is
  lost.
- **Throttle admits up to N per interval.** Each pod's `AtomicLong` gate is
  private → "at most once per second" becomes "at most once per second *per pod*".

### Fixes
1. **Move debounce to the client (best for the keystroke case).** It's node-local
   *and* saves the round trips. This is where debounce belongs anyway.
2. **Key it and coordinate in Redis** when it must be server-side and global:
   - *Throttle*: `SET key NX PX interval` — the pod that wins the `SETNX` fires;
     everyone else (any pod) sees the key exists and drops. Global leading-edge
     throttle in one atomic op.
   - *Debounce*: a coalescing key + a **delay queue** — each event pushes the
     fire time forward (`SET fireAt = now+quiet`); a single delayed worker checks
     "has anyone bumped `fireAt` since?" before firing. Or debounce per-entity
     with sticky routing (fragile — see affinity below).
3. **Sticky sessions** (LB session affinity) can make node-local debounce/throttle
   *appear* to work by pinning a user to one pod — but it defeats load balancing,
   breaks on scale-down/rebalance/pod death, and is not a correctness guarantee.
   Treat as a smell, not a solution.

---

<a name="poc-3"></a>
## POC 3 — Worker pool / queue across replicas

### How it drifts
- **Total concurrency is `N × WORKERS`.** 3 workers/pod × 10 pods = 30 concurrent
  jobs on the downstream — likely far past what "3 workers" was sized to protect.
- **Backpressure is per-pod and dodgeable.** A client that gets `503` from a full
  pod can simply retry and land on a pod with a free slot. The global system never
  actually applied backpressure.
- **Not durable, now multiplied.** Any pod that dies drops *its* in-flight and
  queued jobs; with rolling deploys some pod is always dying.

### Fix — externalize the queue (Option C)
This is the pattern that most wants to leave the process entirely:
- **Durable broker** as the real queue: Kafka, RabbitMQ, SQS, or Postgres
  `SELECT … FOR UPDATE SKIP LOCKED`. Producers enqueue; pods become **consumers**.
- **Concurrency cap becomes global** by controlling total consumer parallelism
  (partition count / prefetch / a shared work-claim), not `N × per-pod`.
- **Backpressure becomes queue depth / consumer lag** — a real global signal.
- **Autoscale on that signal**: **KEDA** scales consumer pods on queue length or
  Kafka lag, so capacity tracks backlog instead of guessing.
- **Durability for free**: broker persists jobs across pod restarts; use consumer
  acks / visibility timeouts so a dead pod's job is redelivered, not lost.

The in-process `ArrayBlockingQueue` is then only a small local smoothing buffer in
front of the broker, if used at all.

---

<a name="poc-4"></a>
## POC 4 — Circuit breaker / retry across replicas

### How it drifts
- **Slow collective reaction.** Each pod must independently rack up its own
  `failureThreshold` before it opens. During an outage, pods that haven't yet hit
  the threshold keep hammering the dead downstream while others have already
  opened — the herd only thins gradually.
- **Uneven probing.** Each `HALF_OPEN` pod sends its own probe → the "one probe"
  guarantee is one-probe-*per-pod*, so a recovering downstream gets `N` probes.
- **Retry amplification is global.** With no shared retry budget, `N` pods each
  retrying `maxAttempts×` turns one downstream blip into an `N × maxAttempts`
  traffic multiplier — exactly the stampede jitter is meant to prevent, now at
  fleet scale.

### Fixes
1. **Per-pod breaker is often *acceptable*** — its primary job is to protect *that
   pod's own threads* from blocking on a dead dependency, which is inherently
   local. Don't over-engineer if that's the goal.
2. **Push it into the service mesh (Option C, best for global behaviour).**
   Istio/Envoy **outlier detection** ejects a bad upstream host fleet-wide, and
   mesh-level **retry budgets** cap total retries as a percentage of traffic — so
   the amplification above can't happen. The concern moves out of your code to the
   sidecar, consistently for every service.
3. **Share breaker state in Redis** if you want app-level global tripping: pods
   report failures to a shared counter and read a shared state, so one pod's
   discovery of an outage opens the breaker for all. Adds a hop and a dependency;
   usually the mesh is the cleaner answer.
4. **Always keep jitter** (it already helps across pods) and **add a retry
   budget** so a broad outage can't be amplified `N×`.

---

## k8s / VM specifics that make this subtle

These apply on top of the per-pattern fixes.

### Horizontal autoscaling changes `N` at runtime
HPA (pods) or an autoscaling group (VMs) changes the replica count under load —
precisely when limits matter most. Any scheme that hard-codes "per-pod = global/N"
silently rescales your global limit every time the fleet grows or shrinks. Global
correctness must not depend on a replica count you don't control. Prefer a shared
store or the edge, where the guarantee is independent of `N`.

### Pods are ephemeral; VMs slightly less so
A pod can be killed, rescheduled, or OOM-evicted at any moment; its heap — and all
node-local state — vanishes. **Never keep state you can't afford to lose in a
pod.** In-flight jobs, "who's rate-limited", breaker state: if it must survive a
restart, it belongs in Redis / a broker / a DB. VMs live longer but the rule is the
same — an autoscaling group scales them in and out too.

### Session affinity is a trap
`sessionAffinity: ClientIP` or sticky cookies can make node-local debounce/throttle
*look* correct by pinning a client to one pod. It trades away even load
distribution, still breaks on scale-in / pod death / rebalancing, and gives no
guarantee during the window a client is remapped. Use it as an optimization, never
as the basis of correctness.

### Clock skew across nodes
Sliding windows, token-bucket refill, and breaker cooldowns all do arithmetic on
"now". Different pods/VMs have slightly different wall clocks (NTP keeps them
close, not identical). Node-local state hides this — each pod only compares its own
clock to itself. The moment you **share** timestamps (e.g. a Redis sorted-set
sliding window written by many pods), skew becomes real: a request can look
"in the future" or expire early. Fixes: use the **store's** clock as the single
source of time (Redis `TIME`, DB `now()`); prefer monotonic durations over
absolute timestamps where possible; keep NTP tight.

### Rolling deploys run two versions at once
During a rollout, old and new pods coexist. If you changed the *shape* of shared
state (Redis key layout, the Lua script, the breaker's state encoding), both
versions read/write it simultaneously → corruption or mutual misreads. Version
shared-state schemas and make changes backward-compatible for one release.

### VM vs pod — same disease, coarser grain
The problem is identical for VMs behind an ELB/HAProxy: each VM holds its own
counters; the fleet size is set by an autoscaling group instead of the HPA; the
fixes are the same (centralize in Redis/DB, externalize queues to a broker, or
decide at the edge). VMs just scale slower and coarser, so the drift is less
twitchy but no less real.

---

## Decision summary

| Pattern | Node-local OK? | Preferred shared mechanism | Ideal home |
|---|---|---|---|
| Rate limit | ✗ (limit × N) | Redis atomic (Lua) counter, or gateway | **Gateway / edge** or Redis |
| Debounce | ✗ (fires per pod) | Client-side, or Redis coalescing key + delay queue | **Client**, else Redis |
| Throttle | ✗ (N× per interval) | Redis `SET NX PX` gate | Redis |
| Worker pool / queue | ✗ (concurrency × N, not durable) | Durable broker + KEDA autoscale on lag | **Broker** (Kafka/SQS/SKIP LOCKED) |
| Circuit breaker | ~ (protects local threads) | Mesh outlier detection, or shared Redis state | **Service mesh** |
| Retry + backoff | ~ (keep jitter) | Mesh retry budget | **Service mesh** (+ jitter, budget) |

**Rule of thumb:** if a pattern protects *this pod's own resource*, node-local is
fine. If its correctness depends on a *global count or a single global decision*,
that count has to live somewhere all pods share — a store, a broker, the mesh, or
the edge — never in a pod's heap.
