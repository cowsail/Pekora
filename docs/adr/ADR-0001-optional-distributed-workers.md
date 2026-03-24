# ADR-0001: Optional Distributed Worker Execution

- Status: Proposed
- Date: 2026-03-23
- Deciders: Pekora maintainers
- Technical Story: Scale step execution horizontally while preserving Pekora's state ownership model

## Context

Pekora currently executes steps in-process from `StepExecutor` through backend adapters, then applies results via `RunEntity`.

This model is simple and reliable, but large deployments need:

- Horizontal scaling of execution workers.
- Fault isolation between scheduling/state and external runtime execution.
- Provider flexibility (Pekko Reliable Delivery first, but not a hard dependency for all users).

At the same time, the framework must preserve current defaults and backward compatibility.

## Decision

Introduce distributed workers as an **optional module set**, with in-process execution remaining the default.

Key points:

1. `RunEntity` remains the sole canonical state owner.
2. Add a dispatch abstraction (`WorkQueueProvider`) in a provider-neutral core module.
3. Keep default behavior unchanged unless `distributedWorkers.enabled=true`.
4. Ship Pekko Reliable Delivery as the first-party default provider implementation.
5. Support pluggable providers behind the same SPI.

## Module Shape

- `runtime:work-dispatch-core` (contracts/SPI, no provider logic)
- `runtime:work-dispatch-pekko` (default provider)
- `runtime:worker-runtime` (claim/lease/execute/report loop)

`runtime:run-engine` depends only on `work-dispatch-core`.

## Consequences

### Positive

- Zero-change default path for existing users.
- Horizontal worker scaling becomes available when enabled.
- Clear extension point for alternate queue technologies.
- Strong consistency model remains intact (engine-side idempotency/dedupe).

### Negative

- Additional operational surface area when enabled (worker fleet, queue tuning, lease metrics).
- More modules and integration paths to test.

### Neutral/Tradeoff

- Transport remains at-least-once; exactly-once effect is achieved in engine via dedupe keys.

## Compatibility

- Backward compatible by default (`enabled=false`).
- Incremental adoption via per-backend or per-step dispatch policy.

## Rejected Alternatives

1. Always-on distributed queue execution.
- Rejected because it forces complexity on users who do not need distributed workers.

2. Let workers update run state directly.
- Rejected because it violates Pekora's single-owner event-sourced state model.

3. Bind run-engine directly to Pekko Reliable Delivery.
- Rejected to preserve framework pluggability.

## Rollout Plan

1. Add `work-dispatch-core` and no-op inline gateway.
2. Integrate dispatch decision into `StepExecutor` with unchanged defaults.
3. Add `work-dispatch-pekko` provider.
4. Add `worker-runtime` and canary by backend.

## Open Questions

- Require policy re-check at worker execution time?
- Persist policy snapshot in every `WorkItem` for audit replay?
- Mandatory provider capabilities for official support tier?

## Links

- Design spec: `docs/DISTRIBUTED_WORKERS_DESIGN.md`
