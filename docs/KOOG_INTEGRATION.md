# Koog Integration Proposal for Pekora

## Recommendation

The cleanest integration is a two-track plan:

1. Use **Koog over A2A first**.
2. Add an **in-process Koog adapter** only if we need lower latency, richer event capture, or tighter JVM-level integration.

That keeps Pekora as the durable control plane and uses Koog as the runtime inside a single `agent` step, which matches Pekora's current adapter model.

## Why This Fits Pekora

Pekora already has the right abstraction boundary:

- `StepExecutor` dispatches a `StepExecutionRequest` to an `AgentRuntimeAdapter`.
- Adapters normalize backend-specific behavior into `StepExecutionResult`.
- The run engine, retries, approvals, and durable workflow state remain in Pekora.

Koog should fit **behind** that boundary. It should not replace Pekora's workflow engine.

The main architectural rule should be:

> Pekora owns cross-step orchestration. Koog owns what happens inside one agent step.

Trying to translate Pekora workflow YAML into Koog strategy graphs would duplicate orchestration, split state ownership, and make retries and approvals ambiguous.

## Integration Options

### Option 1: Koog as an A2A Server

This is the best Phase 1 path.

Koog documents first-class A2A support, including:

- exposing Koog agents as A2A servers
- connecting Koog agents to other A2A agents
- Koog-specific A2A feature modules

Pekora already has an `A2AAdapter`, so the shortest path is:

1. Build a Koog agent service that exposes one or more agents over A2A.
2. Configure a Pekora backend instance that points at that service.
3. Reference that backend from workflow YAML.

Example:

```yaml
agents:
  - id: researcher
    backend: koog-a2a
    config:
      agent_name: researcher
```

```hocon
pekora.adapters.instances {
  koog-a2a {
    type = a2a
    enabled = true
    service-url = "http://koog-service:8081"
    api-key = ""
  }
}
```

#### Why this is the right first step

- No new Pekora runtime model is required.
- No new persistence strategy is required.
- Koog can evolve independently as its own service.
- Pekora can already health check and dispatch to A2A backends.
- This avoids a Kotlin toolchain upgrade inside Pekora on day one.

#### What should be improved in Pekora for this path

The current `A2AAdapter` is usable, but it is still v1-level:

- It flattens `request.input` into plain text lines.
- It only extracts text artifacts from the A2A response.
- It treats execution as single-turn and terminal.

For Koog, I would tighten that adapter in this order:

1. Send structured JSON instead of `"key: value"` text serialization.
2. Pass `runId`, `stepId`, `correlationId`, and agent config explicitly in the A2A payload.
3. Preserve richer A2A artifacts so Pekora can populate `events`, `toolCalls`, and `artifacts`.
4. Later, add support for streamed task updates and map them to Pekora step events.

### Option 2: In-Process Koog Adapter

This is the better long-term path if Koog becomes a major runtime in Pekora.

The shape would be a new module:

- `adapters/koog`

With an adapter roughly like:

```kotlin
class KoogAdapter(...) : AgentRuntimeAdapter {
    override val backendId = "koog"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        // Resolve a registered Koog agent/strategy
        // Map Pekora request -> Koog input
        // Run agent
        // Normalize result back into StepExecutionResult
    }
}
```

There are two reasonable implementation styles:

#### 2A. Build a dedicated `KoogAdapter`

This gives the cleanest ownership model.

- Pekora owns adapter lifecycle.
- Koog-specific config lives in one place.
- We can directly map Koog events, tools, persistence checkpoints, and tracing metadata.

#### 2B. Build Koog agents on top of the existing `native` backend

This is lower-effort if we want a proof of concept quickly.

- Register Koog-backed handlers through `NativeAgentRegistry`.
- Use `PER_RUN` for agents that maintain conversation or tool-loop state.
- Use per-invocation handlers for stateless Koog calls.

This is a good prototype path, but I would still expect a real `koog` adapter later if the integration becomes strategic.

## Important Constraint: Kotlin Version

Koog's docs currently say:

- JDK 17+
- Kotlin 2.2.0+
- Gradle 8.0+

Pekora now uses Kotlin `2.2.0` in the root build.

That makes the A2A path especially attractive because it avoids forcing a repo-wide Kotlin upgrade before we prove the value of the integration.

If we want in-process Koog soon, the first technical question is whether Pekora is willing to move to Kotlin 2.2+.

## What Koog Features Map Well to Pekora

### Strong fits

- **Graph-based agents**
  - Good for implementing a single complex step with an internal tool loop.
- **Structured output**
  - Good for producing deterministic step outputs that map cleanly into `StepExecutionResult.output`.
- **OpenTelemetry**
  - Good for joining Koog spans with Pekora run and step correlation IDs.
- **A2A**
  - Good transport layer for a remote Koog runtime.

### Careful fits

- **Agent persistence**
  - Useful inside a step or for run-scoped conversational memory.
  - Risky if it becomes a second source of truth for workflow recovery.
- **Streaming API**
  - Valuable, but Pekora's current adapter contract returns a single terminal `StepExecutionResult`.
  - To benefit fully, Pekora needs a step-progress event channel.

## Proposed Ownership Model

### Pekora should remain authoritative for

- workflow definitions
- run lifecycle
- retries
- approval gates
- cancellation semantics
- canonical audit timeline

### Koog should own

- prompt construction inside the agent
- tool-loop execution inside the step
- model/provider integration
- optional run-scoped memory inside the agent
- fine-grained LLM and tool tracing

## Data Mapping

### Pekora -> Koog

Recommended mapping:

- `StepExecutionRequest.definitionRef` -> Koog agent or strategy identifier
- `request.input` -> structured Koog input object
- `request.context` -> memory/session metadata
- `request.constraints.timeoutSeconds` -> execution timeout
- `request.constraints.budget.maxTokens` -> model/token budget hint
- `request.correlationId` -> tracing context

### Koog -> Pekora

Recommended mapping:

- final assistant or structured result -> `StepExecutionResult.output`
- tool activity -> `toolCalls`
- streamed or lifecycle updates -> `events`
- documents/files/URLs -> `artifacts`
- model usage and duration -> `metrics`

## Persistence Strategy

Default rule:

- Pekora persists the workflow run.
- Koog persistence is optional and local to the adapter/runtime.

I would not make Koog checkpoints part of Pekora's primary recovery path in Phase 1.

Instead:

- For remote Koog over A2A: let Koog manage its own internal continuity if needed.
- For in-process Koog: keep Koog state scoped to `(runId, agentId)`.
- If Koog persistence is enabled, treat it as an optimization for agent continuity, not as the canonical workflow journal.

## Observability Strategy

Koog has OpenTelemetry support, which is a strong fit for Pekora.

I would propagate these attributes into Koog spans:

- `pekora.run_id`
- `pekora.step_id`
- `pekora.backend`
- `pekora.workflow_name`
- `pekora.workflow_version`
- `pekora.correlation_id`

That would let Pekora continue to own high-level run visibility while Koog provides detailed LLM/tool spans underneath.

## Suggested Delivery Plan

### Phase 1: A2A bridge

- Stand up a small Koog service exposing one agent over A2A.
- Add a Pekora adapter instance like `koog-a2a`.
- Improve `A2AAdapter` request/response mapping for structured payloads.
- Validate one real workflow step end to end.

### Phase 2: Better result fidelity

- Capture structured outputs, tool calls, and artifacts from Koog responses.
- Add correlation propagation and OpenTelemetry alignment.
- Decide whether streamed task updates should appear in Pekora projections.

### Phase 3: Native JVM embedding

- Upgrade Pekora to Kotlin 2.2+ if needed.
- Add `adapters/koog`.
- Support run-scoped Koog agents and richer lifecycle management.

### Phase 4: Advanced step semantics

- Stream step progress from Koog into Pekora events.
- Reuse Koog agent state across multiple steps when explicitly configured.
- Add backend-specific validation for Koog agent definitions.

## Recommended First Implementation

If the goal is to get value quickly, I would do this:

1. Keep Pekora unchanged at the orchestration layer.
2. Treat Koog as a remote runtime exposed over A2A.
3. Make only targeted improvements to `A2AAdapter`.
4. Use Koog structured output for deterministic step contracts.
5. Revisit an in-process adapter only after we validate the runtime fit.

That is the fastest route with the least architectural regret.

## Open Questions

These answers materially change the design:

1. Do you want Koog to run **inside** the Pekora JVM, or is a separate Koog service acceptable?
2. Do you want to stay on Kotlin `2.2.x`, or move to a newer Kotlin line before adding `adapters/koog`?
3. Do you want Koog to power entire workflows, or only individual `agent` steps inside Pekora workflows?
4. Is preserving Koog streaming/progress data in Pekora important for the first version?
5. Do you need long-lived conversational state across multiple Pekora steps, or is each Koog invocation independent?
6. Is A2A already part of your interoperability plan, or would you rather avoid it and go straight to an in-process adapter?

## References

- Koog overview: <https://docs.koog.ai/>
- Koog basic agents: <https://docs.koog.ai/agents/basic-agents/>
- Koog graph-based agents: <https://docs.koog.ai/agents/graph-based-agents/>
- Koog structured output: <https://docs.koog.ai/structured-output/>
- Koog agent persistence: <https://docs.koog.ai/agent-persistence/>
- Koog streaming API: <https://docs.koog.ai/streaming-api/>
- Koog OpenTelemetry support: <https://docs.koog.ai/opentelemetry-support/>
- Koog A2A protocol overview: <https://docs.koog.ai/a2a-protocol-overview/>
- Koog A2A integration: <https://docs.koog.ai/a2a-koog-integration/>
