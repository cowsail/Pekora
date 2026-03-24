# Pekora Agent Workflow Framework

A distributed, durable workflow framework for AI agents where Pekko owns workflow execution and external runtimes like LangGraph, A2A agents, and Bedrock AgentCore plug in behind stable adapter contracts.

## What It Is

A Pekko-based runtime and framework that turns AI agent workflows into:

- **Versioned workflow artifacts** — immutable, publishable workflow definitions
- **Durable run state machines** — event-sourced, replayable, resumable
- **Distributed executions** — cluster-sharded across nodes
- **Governed and observable operations** — policies, approvals, timelines, metrics

## What It Is Not

- Not a prompt chaining library
- Not a single-vendor managed service
- Not a replacement for agent runtimes like LangGraph — it is the **durable control plane** around them

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Control Plane                         │
│      Optional HTTP API Plugin (templates, runs, approvals)│
└─────────────┬───────────────────────────┬───────────────┘
              │                           │
┌─────────────▼───────────┐   ┌───────────▼───────────────┐
│    Definition Plane      │   │     Runtime Plane          │
│  WorkflowRegistry        │   │  RunEntity (sharded)       │
│  Templates & Versions    │   │  StepExecutor              │
│  PolicyGuard             │   │  ApprovalManager           │
└──────────────────────────┘   └───────────┬───────────────┘
                                           │
                               ┌───────────▼───────────────┐
                               │     Adapter Plane          │
                               │  LangGraphAdapter          │
                               │  A2AAdapter                │
                               │  BedrockAgentCoreAdapter   │
                               │  NativeAdapter             │
                               │  GenericAdapter            │
                               └───────────────────────────┘
                                           │
                               ┌───────────▼───────────────┐
                               │   Observability Plane      │
                               │  RunProjectionStore        │
                               │  Summaries & Timelines     │
                               └───────────────────────────┘
```

## Project Structure

```
pekora/
├── sdk/
│   ├── dsl/              # Domain models, run events, YAML parser
│   └── client/           # HTTP client SDK
├── adapters/
│   ├── common/           # Adapter interface (AgentRuntimeAdapter)
│   ├── langgraph/        # LangGraph remote execution adapter
│   ├── a2a/              # A2A protocol adapter
│   ├── bedrock-agentcore/# Amazon Bedrock AgentCore adapter
│   ├── native/           # In-process native agent adapter
│   └── generic/          # Generic HTTP or in-process actor adapter
├── runtime/
│   ├── run-engine/       # RunEntity, StepExecutor, ApprovalManager
│   ├── framework/        # Framework bootstrap + in-process client API
│   ├── workflow-registry/# WorkflowRegistry actor
│   ├── policy/           # PolicyGuard
│   ├── projection/       # Read-model projections
│   └── api/              # Optional HTTP plugin + server entrypoint
├── examples/
│   └── issue-to-pr/      # End-to-end example workflow
├── docs/                 # Design spec, next steps
└── infra/                # Docker, Kubernetes configs
```

## Requirements

- **JDK 21** or later
- **Gradle 8.11+** (wrapper included)

## Quick Start

### Build

```bash
./gradlew build
```

### Embed the Framework

```kotlin
import org.pekora.dsl.WorkflowParser
import org.pekora.framework.CreateRunSpec
import org.pekora.framework.PekoraFramework
import org.pekora.framework.PekoraFrameworkOptions

val handle = PekoraFramework.start(
    options = PekoraFrameworkOptions(
        adapters = mapOf("my-backend" to myAdapter),
    ),
)

val client = handle.client()
client.createTemplate(id = "hello", name = "Hello").toCompletableFuture().get()
client.publishVersion("hello", 1, WorkflowParser.parse(workflowYaml)).toCompletableFuture().get()
client.createRun(CreateRunSpec(templateId = "hello")).toCompletableFuture().get()
```

### Run the Optional HTTP Server

```bash
./gradlew :runtime:api:run
```

Or with environment variables:

```bash
HTTP_HOST=0.0.0.0 HTTP_PORT=8080 ./gradlew :runtime:api:run
```

The server starts on `http://localhost:8080` with the following endpoints:

### API Endpoints

#### Workflow Templates
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/workflow-templates` | Register a new workflow template |
| `GET` | `/workflow-templates` | List all templates |
| `POST` | `/workflow-templates/{id}/versions` | Publish a workflow version |
| `GET` | `/workflow-versions/{templateId}:{version}` | Get a specific version |

#### Runs
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/runs` | Create and start a new run |
| `GET` | `/runs/{runId}` | Get run status |
| `POST` | `/runs/{runId}/cancel` | Cancel a run |
| `POST` | `/runs/{runId}/resume` | Resume a paused run |

#### Approvals
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/approvals` | List pending approvals |
| `POST` | `/approvals/{approvalId}/approve` | Approve a checkpoint |
| `POST` | `/approvals/{approvalId}/reject` | Reject a checkpoint |

## Workflow DSL

Workflows are defined in YAML. Here's a minimal example:

```yaml
workflow:
  name: issue-to-pr
  version: 1

  inputs:
    type: object
    required: [repo, issue_id]

  agents:
    - id: planner
      backend: langgraph
      model_profile: planning-default

    - id: coder
      backend: a2a-default
      model_profile: coding-default

  steps:
    - id: plan
      type: agent
      agent: planner
      input:
        repo: "${inputs.repo}"
        issue_id: "${inputs.issue_id}"
      next: implement

    - id: implement
      type: agent
      agent: coder
      input:
        plan: "${steps.plan.output}"
      next: done

    - id: done
      type: result
      output:
        implementation: "${steps.implement.output}"
```

### Step Types

| Type | Description |
|------|-------------|
| `agent` | Execute via an agent runtime adapter (LangGraph, A2A, Bedrock AgentCore, native, etc.) |
| `decision` | Branch based on conditions |
| `approval` | Pause for human approval |
| `parallel` | Execute steps concurrently (Phase 4) |
| `subworkflow` | Invoke a nested workflow (Phase 4) |
| `wait` | Wait for an external event |
| `result` | Capture final output and complete the workflow |

### Expression References

Step inputs can reference prior outputs using `${...}` expressions:

- `${inputs.field}` — workflow input field
- `${steps.stepId.output}` — full output of a prior step
- `${steps.stepId.output.field}` — specific field from a prior step's output

### Retries

```yaml
retries:
  max_attempts: 3
  backoff_ms: 2000
  multiplier: 2.0
```

### Policies

```yaml
policies:
  - id: strict-policy
    inline:
      allowed_backends: [langgraph]
      timeout_seconds: 300
      max_tokens: 50000
      require_approval: true
```

## Core Concepts

### RunEntity
The event-sourced persistent actor that owns a single workflow run. Keyed by `runId`, sharded across the Pekko cluster. It is the **canonical source of truth** — external runtimes may execute steps, but they never own global workflow state, retry semantics, approval lifecycle, or the event timeline.

### Run States
`Created` → `LoadingDefinition` → `Ready` → `Executing` → `Completed` / `Failed` / `Cancelled`

Intermediate states: `WaitingForApproval`, `WaitingForExternalEvent`

### Event Sourcing
Every run persists a full event journal:
- `RunCreated`, `WorkflowLoaded`, `RunStarted`
- `StepScheduled`, `StepStarted`, `StepCompleted`, `StepFailed`, `StepRetryScheduled`
- `ApprovalRequested`, `ApprovalReceived`
- `RunPaused`, `RunResumed`, `RunCompleted`, `RunFailed`, `RunCancelled`

This provides replayable history, audit trails, deterministic recovery, and clear timelines.

### Adapters
External runtimes plug in through a single adapter contract:

- **AgentRuntimeAdapter** — `executeStep(StepExecutionRequest): CompletionStage<StepExecutionResult>`

Five implementations are provided:
- `LangGraphAdapter` — remote LangGraph (Python) service via HTTP
- `A2AAdapter` — generic A2A JSON-RPC protocol adapter
- `BedrockAgentCoreAdapter` — Amazon Bedrock AgentCore runtime adapter
- `NativeAdapter` — in-process Pekko actor/per-invocation agent adapter
- `GenericAdapter` — configurable HTTP or in-process actor mode

Tools and skills are the agent runtime's concern — Pekora does not mediate tool calls. Agent runtimes may optionally report tool calls in `StepExecutionResult.toolCalls` (as `ToolCallRecord`) for audit purposes.

- **WorkspaceAdapter** — manages workspace lifecycle (repo checkout, sandbox) — future use

### PolicyGuard
Evaluates whether a step or call is allowed based on:
- Allowed backends, allowed models
- Cost budgets and timeout ceilings
- Side-effect classifications (`read_only`, `write_scoped`, `external_side_effect`, `high_risk`)
- Approval requirements

## Pekko Modules Used

- **Pekko Typed** — typed actor system
- **Pekko Cluster** — cluster membership
- **Pekko Cluster Sharding** — RunEntity distribution
- **Pekko Persistence** — event sourcing for RunEntity
- **Pekko HTTP** — REST API
- **Pekko Serialization (Jackson)** — event/state serialization

## Client SDK

A Java/Kotlin HTTP client is provided in `sdk/client`:

```kotlin
val client = FrameworkClient("http://localhost:8080")

// Register a template
client.createTemplate("issue-to-pr", "Issue to PR Workflow")

// Publish a version
client.publishVersion("issue-to-pr", 1, workflowYaml)

// Start a run
client.createRun("issue-to-pr", inputs = mapOf("repo" to "org/repo", "issue_id" to "42"))

// Check status
client.getRunStatus("run_abc123")

// Approve a checkpoint
client.approve("approval_xyz", approver = "team-lead")
```

## Roadmap

See [docs/NEXT_STEPS.md](docs/NEXT_STEPS.md) for the full phased implementation plan:

- **Phase 2**: Adapter integration (wire adapters to real services, permission mapping)
- **Phase 3**: LangGraph integration (Python service, event streaming, schema validation)
- **Phase 4**: Parallelism and subworkflows (fan-out/fan-in, nested runs)
- **Phase 5**: Hardening (snapshots, database projections, conformance tests)
- **Phase 6**: Multi-tenancy and security
- **Phase 7**: Agent-authored workflows

## License

TBD
