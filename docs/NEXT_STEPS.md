# Next Steps — Pekko Agent Workflow Framework

## Phase 2: OpenClaw Integration

### Goals
- Wire OpenClawToolAdapter and OpenClawSkillAdapter to real OpenClaw service endpoints
- Implement permission mapping between framework policies and OpenClaw's authorization model
- Add workspace-oriented skill support via WorkspaceAdapter (repo checkout, sandbox prep)

### Tasks
- [ ] Define OpenClaw service discovery configuration (service URL, auth tokens)
- [ ] Implement OpenClaw authentication handshake in both adapters
- [ ] Map PolicyGuard's `allowedTools` / `allowedSkills` to OpenClaw's permission model
- [ ] Add integration tests with a mock OpenClaw service
- [ ] Implement WorkspaceAdapter for repo checkout and sandbox lifecycle
- [ ] Add health-check endpoints for adapter connectivity
- [ ] Document OpenClaw tool/skill registration workflow

---

## Phase 3: LangGraph Integration

### Goals
- Stand up a Python LangGraph service that hosts registered graphs
- Complete the LangGraphAdapter with streaming event support
- Validate state mapping and output schema enforcement

### Tasks
- [ ] Create Python LangGraph service scaffold (FastAPI + LangGraph)
- [ ] Define the `/execute` and `/cancel` HTTP contracts
- [ ] Implement graph registration endpoint (register named graphs with schemas)
- [ ] Add event streaming support (SSE or WebSocket) from LangGraph to framework
- [ ] Implement state mapping: framework StepExecutionRequest → LangGraph graph state
- [ ] Implement result normalization: LangGraph output → StepExecutionResult
- [ ] Add schema validation on LangGraph output against declared workflow output schema
- [ ] Add failure classification (transient vs permanent) in the adapter
- [ ] Write integration tests with a sample LangGraph graph
- [ ] Add Docker Compose setup for framework + LangGraph service

---

## Phase 4: Parallelism and Subworkflows

### Goals
- Support parallel step execution with fan-out and fan-in
- Support subworkflow invocation (workflow-within-workflow)

### Tasks
- [ ] Extend RunEntity to handle `StepKind.PARALLEL` — spawn concurrent step executions
- [ ] Implement fan-in logic: wait for all parallel branches to complete before advancing
- [ ] Handle partial failure in parallel branches (fail-fast vs wait-all policies)
- [ ] Extend RunState to track multiple concurrently running steps
- [ ] Implement `StepKind.SUBWORKFLOW` — create child RunEntity, link lifecycle
- [ ] Propagate cancellation from parent to child runs
- [ ] Propagate subworkflow output back to parent step output
- [ ] Add parallel + subworkflow steps to the DSL parser
- [ ] Update projections to show parallel branch status
- [ ] Add timeline entries for fork/join events
- [ ] Write tests for parallel execution ordering guarantees

---

## Phase 5: Hardening

### Goals
- Production readiness: snapshots, improved projections, policy testing, adapter conformance

### Tasks

### Snapshots
- [ ] Implement Pekko Persistence snapshot strategy for RunEntity (every N events or time-based)
- [ ] Add snapshot serialization tests (roundtrip RunState through Jackson)
- [ ] Verify recovery from snapshots + replay of subsequent events
- [ ] Benchmark replay performance with/without snapshots for long-running workflows

### Projections
- [ ] Replace in-memory RunProjectionStore with database-backed projections
- [ ] Implement Pekko Projections event handler for RunEvents → read model tables
- [ ] Add projection for cost/token metrics aggregation
- [ ] Add projection for per-step latency percentiles
- [ ] Add projection for failure rate by backend/adapter
- [ ] Implement projection health monitoring and offset tracking

### Policy Testing
- [ ] Add policy validation API endpoint (POST /validation/workflow)
- [ ] Implement dry-run mode that evaluates policies without executing steps
- [ ] Add policy composition tests (multiple overlapping policies)
- [ ] Add side-effect classification enforcement (block high_risk without approval)

### Adapter Conformance
- [ ] Define adapter conformance test suite (interface contract tests)
- [ ] Implement test harness that runs conformance suite against any AgentRuntimeAdapter
- [ ] Add timeout enforcement tests (adapter exceeds allowed timeout)
- [ ] Add error handling tests (adapter throws, returns malformed results)
- [ ] Run conformance suite against LangGraphAdapter, OpenClawToolAdapter, OpenClawSkillAdapter

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

---

## Phase 7: Agent-Authored Workflows (Future)

### Tasks
- [ ] Define workflow draft API (agents can submit workflow YAML for validation)
- [ ] Implement structural validation (step graph connectivity, reference resolution)
- [ ] Implement policy validation (proposed workflow vs allowed policies)
- [ ] Add dry-run / simulation mode
- [ ] Add publication gates (human approval before workflow goes live)
- [ ] Add provenance metadata (who/what created the workflow, revision history)
- [ ] Implement workflow diffing for version comparison

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

- [ ] Add comprehensive unit tests for WorkflowParser (edge cases, malformed YAML)
- [ ] Add unit tests for RunEntity state transitions (all event types)
- [ ] Add unit tests for PolicyGuard (policy composition, edge cases)
- [ ] Add unit tests for expression resolution (`${inputs.x}`, `${steps.y.output.z}`)
- [ ] Replace `Map<String, String>` with typed input/output models where appropriate
- [ ] Add structured error types instead of string error messages
- [ ] Add request correlation ID propagation through all log messages
- [ ] Add metrics collection (Micrometer or Pekko metrics)
- [ ] Consider moving from in-memory journal to JDBC or R2DBC persistence plugin
