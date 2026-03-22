# Next Steps — Pekora Agent Workflow Framework

## Phase 2: Adapter Integration ✅ Complete

### Completed
- [x] HOCON config (`pekora.adapters`) with env var overrides for all adapter URLs and auth
- [x] `AdapterFactory` reads config and constructs only enabled adapters at startup
- [x] Rewrote `LangGraphAdapter` for AgentServer thread + run model (`X-Api-Key` auth, async polling)
- [x] Replaced `OpenClawAdapter` with `A2AAdapter` (JSON-RPC 2.0 `message/send`, Bearer auth, agent card discovery)
- [x] Replaced `StrandsAdapter` with `BedrockAgentCoreAdapter` (SigV4 + OAuth auth, AgentCore invocations API)
- [x] Updated `GenericAdapter` with `apiKey` support and `healthCheck()` implementation
- [x] Added `healthCheck()` to `AgentRuntimeAdapter` interface with `AdapterHealth` / `HealthStatus` types
- [x] Added `GET /health/adapters` route via `HealthRoutes`
- [x] Wired `PolicyGuard` enforcement into `StepExecutor` before adapter dispatch
- [x] Updated `RunEntity` to pass `stepDefinition`, `agents`, and `stepPolicies` to `ExecuteStep`
- [x] Implemented `LocalWorkspaceAdapter` (`git-checkout` via `ProcessBuilder`, recursive cleanup)
- [x] Integration tests for all adapters (mock `HttpServer`), `PolicyGuard`, and `HealthRoutes`
- [x] Created `docs/ADAPTER_CONFIGURATION.md`

---

## Phase 3: LangGraph Streaming + Thread Reuse

### Goals
- SSE streaming for long-running LangGraph steps (instead of polling)
- Thread reuse across steps within the same run (pass `thread_id` via run context)
- Validate state mapping and output schema enforcement

### Tasks
- [ ] Implement SSE streaming in `LangGraphAdapter` (deferred from Phase 2)
- [ ] Store `thread_id` in `RunState` and reuse across steps referencing the same agent
- [ ] Define the LangGraph output → `StepExecutionResult` schema mapping contract
- [ ] Add failure classification (transient vs. permanent) in `LangGraphAdapter`
- [ ] Write integration tests with a sample LangGraph graph against a live AgentServer

---

## Phase 3.5: Koog Integration

### Goals
- Integrate Koog as a runtime backend without changing Pekora orchestration ownership
- Validate A2A-first integration path and capture richer Koog execution metadata
- Prepare for in-process Koog adapter once Kotlin/toolchain prerequisites are met

### Tasks
- [ ] Track design decisions from [Koog integration proposal](./KOOG_INTEGRATION.md)
- [ ] Add `koog-a2a` adapter instance to environment configs and example workflow
- [ ] Improve `A2AAdapter` payload mapping to send structured input/context metadata
- [ ] Map Koog/A2A artifacts into `StepExecutionResult.events`, `toolCalls`, and `artifacts`
- [ ] Propagate correlation metadata (`runId`, `stepId`, `correlationId`) into Koog calls
- [ ] Add an integration test against a Koog A2A service fixture
- [x] Upgrade Kotlin toolchain to `2.2+` to unblock in-process Koog adapter work
- [ ] Create `adapters/koog` module skeleton (`KoogAdapter : AgentRuntimeAdapter`)

---

## Phase 4: Parallelism and Subworkflows

### Goals
- Support parallel step execution with fan-out and fan-in
- Support subworkflow invocation (workflow-within-workflow)

### Tasks
- [ ] Extend `RunEntity` to handle `StepKind.PARALLEL` — spawn concurrent step executions
- [ ] Implement fan-in logic: wait for all parallel branches to complete before advancing
- [ ] Handle partial failure in parallel branches (fail-fast vs wait-all policies)
- [ ] Extend `RunState` to track multiple concurrently running steps
- [ ] Implement `StepKind.SUBWORKFLOW` — create child `RunEntity`, link lifecycle
- [ ] Propagate cancellation from parent to child runs
- [ ] Propagate subworkflow output back to parent step output
- [ ] Add parallel + subworkflow steps to the DSL parser
- [ ] Update projections to show parallel branch status
- [ ] Write tests for parallel execution ordering guarantees

---

## Phase 5: Hardening

### Goals
- Production readiness: snapshots, improved projections, policy testing, adapter conformance

### Snapshots
- [ ] Implement Pekko Persistence snapshot strategy for `RunEntity` (every N events or time-based)
- [ ] Add snapshot serialization tests (roundtrip `RunState` through Jackson)
- [ ] Verify recovery from snapshots + replay of subsequent events
- [ ] Benchmark replay performance with/without snapshots for long-running workflows

### Projections
- [ ] Replace in-memory `RunProjectionStore` with database-backed projections
- [ ] Implement Pekko Projections event handler for `RunEvent`s → read model tables
- [ ] Add projection for cost/token metrics aggregation
- [ ] Add projection for per-step latency percentiles
- [ ] Add projection for failure rate by backend/adapter
- [ ] Implement projection health monitoring and offset tracking

### Policy Testing
- [ ] Add policy validation API endpoint (`POST /validation/workflow`)
- [ ] Implement dry-run mode that evaluates policies without executing steps
- [ ] Add side-effect classification enforcement (block `HIGH_RISK` without approval)

### Adapter Conformance
- [ ] Define adapter conformance test suite (interface contract tests)
- [ ] Implement test harness that runs conformance suite against any `AgentRuntimeAdapter`
- [ ] Add timeout enforcement tests (adapter exceeds allowed timeout)
- [ ] Run conformance suite against `LangGraphAdapter`, `A2AAdapter`, `BedrockAgentCoreAdapter`, `GenericAdapter`

### Approval UX
- [ ] Add approval expiration (auto-reject after timeout)
- [ ] Add approval delegation (reassign approver)
- [ ] Add approval audit trail in projections
- [ ] Consider WebSocket/SSE endpoint for real-time approval notifications

---

## Phase 6: Multi-Tenancy and Security (Future)

### Tasks
- [ ] Implement tenant-scoped workflow registries
- [ ] Add tenant isolation in cluster sharding (tenant-prefixed entity IDs)
- [ ] Implement secrets binding in control plane (vault integration)
- [ ] Add backend restrictions per tenant
- [ ] Add sandbox isolation for workspace execution
- [ ] Add rate limiting per tenant
- [ ] Add audit logging for all API operations
- [ ] Add caller identity model (`subject`, `tenant`, `roles`, `scopes`) to run/workflow API requests
- [ ] Propagate caller identity through `RunEntity`/`StepExecutor` as immutable execution context
- [ ] Extend `PolicyGuard` to validate every backend/agent/tool/skill invocation against caller identity
- [ ] Persist policy decision traces (allow/deny + matched rules + caller context + step metadata)
- [ ] Add a policy decision query API for auditability (`who ran what`, `why allowed/blocked`)
- [ ] Define pluggable authn/authz providers (JWT/OIDC, mTLS principal, static API key, custom)
- [ ] Implement first auth provider: JWT/OIDC (reference implementation of provider SPI)
- [ ] Add signed correlation between API caller identity and emitted run/step events
- [ ] Optional: materialize policy decision traces into a query-optimized read model

---

## Phase 7: Agent-Authored Workflows (Future)

### Tasks
- [ ] Define workflow draft API (agents can submit workflow YAML for validation)
- [ ] Implement structural validation (step graph connectivity, reference resolution)
- [ ] Implement policy validation (proposed workflow vs. allowed policies)
- [ ] Add dry-run / simulation mode
- [ ] Add publication gates (human approval before workflow goes live)
- [ ] Add provenance metadata (who/what created the workflow, revision history)
- [ ] Implement workflow diffing for version comparison

---

## Phase 8: Plugin Platform + UI (Future)

### Goals
- Support installable/activatable framework plugins with lifecycle, permissions, and versioning
- Allow first-party and third-party capabilities (UI, adapters, routes, projections, policy packs)
- Ship a first plugin that provides workflow/run visualization and operational dashboards

### Tasks
- [ ] Define plugin manifest and capability model (id, version, permissions, extension points)
- [ ] Implement plugin install/activate/deactivate/uninstall lifecycle APIs
- [ ] Add plugin isolation boundaries (start with in-process classloader boundary; evaluate out-of-process isolation later)
- [ ] Define plugin compatibility contract (framework API version + migration policy)
- [ ] Add plugin registry/discovery mechanism (local + remote catalog)
- [ ] Add plugin audit events (installed by whom, activated when, version changes)
- [ ] Implement extension points for adapters, API routes, projections, and policy modules
- [ ] Create a reference `ui-observability` plugin
- [ ] `ui-observability`: visualize workflow definitions/graphs
- [ ] `ui-observability`: show active and historical runs with timelines
- [ ] `ui-observability`: show adapter health and policy decision traces
- [ ] `ui-observability`: keep v1 read-only (future: opt-in control actions pause/resume/cancel/approve)
- [ ] Add plugin SDK docs and conformance tests
- [ ] Start plugin distribution with local filesystem installs; defer remote registry/catalog

---

## Infrastructure Tasks

- [ ] Add Dockerfile for the framework server
- [ ] Add Docker Compose with framework + in-memory persistence
- [ ] Add Kubernetes manifests (Deployment, Service, ConfigMap)
- [ ] Add Pekko Cluster Bootstrap configuration for K8s
- [ ] Add CI/CD pipeline (build, test, publish Docker image)
- [ ] Add Grafana dashboard templates for framework metrics
- [ ] Add OpenTelemetry instrumentation for distributed tracing

---

## Technical Debt / Improvements

- [ ] Add comprehensive unit tests for `WorkflowParser` (edge cases, malformed YAML)
- [ ] Add unit tests for `RunEntity` state transitions (all event types)
- [ ] Add unit tests for expression resolution (`${inputs.x}`, `${steps.y.output.z}`)
- [ ] Replace `Map<String, String>` with typed input/output models where appropriate
- [ ] Add structured error types instead of string error messages
- [ ] Add request correlation ID propagation through all log messages
- [ ] Add metrics collection (Micrometer or Pekko metrics)
- [ ] Consider moving from in-memory journal to JDBC or R2DBC persistence plugin
- [ ] Wire `LocalWorkspaceAdapter` into `StepExecutor` for workspace-aware steps (Phase 3)
