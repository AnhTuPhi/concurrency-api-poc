# ISSUE — Keeping an API alive under concurrent load and unreliable dependencies

## Context

A financial API (accounts, statements, orders) does not fail politely. When it
is overwhelmed it does not "get slow for a bit" — it tips over: threads pile up
waiting on a stuck dependency, memory balloons with buffered work, and a single
noisy client degrades service for everyone. In a trading/settlement context that
is not a cosmetic problem; it is lost orders, duplicated side effects, and SLA
breaches.

The traffic we actually receive is not smooth:

- **Bursts.** UI retries, batch jobs, mobile clients reconnecting after a network
  blip, and the occasional script all arrive in spikes, not at a steady rate.
- **Abusive or buggy callers.** A client stuck in a tight retry loop can send
  hundreds of requests per second at one endpoint.
- **Expensive work per request.** Some calls fan out to Oracle, Elasticsearch,
  Kafka, or an external identity provider. One inbound request can cost far more
  downstream than it looks.
- **Flaky / slow dependencies.** Downstreams have outages, GC pauses, and tail
  latency. When they misbehave, naive callers make it *worse* by retrying
  immediately and in lockstep.

## The problem

Without deliberate concurrency control, four failure modes recur:

1. **Overload.** No cap on inbound rate → a burst or a single abusive client
   saturates CPU, connection pools, and downstreams. Everyone's latency spikes.
2. **Redundant work.** The same intent arrives many times in a short window
   (keystrokes into a search box, a spam-clicked "Submit", duplicate webhooks).
   Doing the work every time is wasted cost and can cause duplicate side effects.
3. **Unbounded buffering.** Accepting more work than we can process, then
   queueing it in memory without a limit, trades a fast failure for a slow,
   total one (OOM, or latency that grows without bound).
4. **Cascading failure.** A failing downstream stalls request threads; those
   threads are a finite resource; when they are exhausted the *whole* service is
   down — taking out healthy endpoints that never needed the broken dependency.
   Immediate, synchronized retries turn a blip into a self-inflicted DDoS.

## What we are protecting

- **Downstream capacity** — Oracle, Elasticsearch, Kafka, MinIO, VN-ID — from
  being hammered past their safe throughput.
- **Our own finite resources** — request threads, heap, connection pools.
- **Fairness** — one client must not be able to starve the rest.
- **Correctness of intent** — "the last thing the user meant" should win; work
  should not be silently duplicated.
- **Blast radius** — one broken dependency must not take down unrelated endpoints.

## Goal of this POC

Demonstrate, in one small runnable Spring Boot app, the four families of
in-process concurrency-control patterns that address the failure modes above, so
the team can *see* each one behave under load and reason about its trade-offs:

| Failure mode | Pattern family | POC |
|---|---|---|
| Overload | **Rate limiting** — token bucket, sliding window | POC 1 |
| Redundant work | **Debounce / throttle** — collapse floods of events | POC 2 |
| Unbounded buffering | **Worker pool + bounded queue** — backpressure | POC 3 |
| Cascading failure | **Circuit breaker + retry with backoff/jitter** | POC 4 |

## Explicit non-goals (scope boundary)

This POC deliberately keeps all state **in-process, in a single JVM** so the
algorithms are visible and dependency-free. It does **not** solve the
distributed version of any of these problems. That boundary is the whole point of
the follow-up analysis:

- Correctness of each pattern when the app runs as **multiple replicas** (k8s
  pods / VMs behind a load balancer) is out of scope for the code but is
  addressed in [CONSISTENCY.md](CONSISTENCY.md).
- Production concerns (durability, eviction, metrics, tracing, auth) are
  acknowledged as tech debt per pattern in [TECHNICAL.md](TECHNICAL.md).

## Definition of done

- Each pattern is implemented from first principles (no Resilience4j / Bucket4j)
  so the mechanics are legible.
- A single web page ([`/`](src/main/resources/static/index.html)) lets you spam
  each pattern and watch it react in real time.
- An explainer page ([`/explain.html`](src/main/resources/static/explain.html))
  diagrams the flow and key tech of each pattern and the scaling story.
- Docs state, per pattern: the hard problem, what it protects, the solution
  shape, the key tech by responsibility, and the tech debt to acknowledge.
