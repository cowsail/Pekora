# Testing Guide — Pekko Agent Workflow Framework

## Overview

This guide covers how to test the framework at every level: unit tests, integration tests, end-to-end workflow tests, and manual API testing.

## Prerequisites

- JDK 21+
- The project builds successfully: `./gradlew build`

## Running Tests

### All Tests

```bash
./gradlew test
```

### Specific Module

```bash
./gradlew :sdk:dsl:test
./gradlew :runtime:run-engine:test
./gradlew :runtime:policy:test
./gradlew :adapters:langgraph:test
```

### With Verbose Output

```bash
./gradlew test --info
```

---

## Unit Tests to Write

The framework ships with compilable source but needs test coverage. Below is a guide for writing tests for each module.

### 1. DSL Parser Tests

**Module**: `sdk/dsl`
**Test location**: `sdk/dsl/src/test/kotlin/org/pekkoagent/dsl/`

Test the YAML-to-domain-model parser:

```kotlin
package org.pekora.dsl

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertFailsWith

class WorkflowParserTest {

    @Test
    fun `parse minimal workflow`() {
        val yaml = """
            workflow:
              name: test-workflow
              version: 1
              steps:
                - id: start
                  type: result
                  output:
                    message: "done"
        """.trimIndent()

        val definition = WorkflowParser.parse(yaml)
        assertEquals("test-workflow", definition.name)
        assertEquals(1, definition.version)
        assertEquals(1, definition.steps.size)
        assertEquals(StepKind.RESULT, definition.steps[0].type)
    }

    @Test
    fun `parse workflow with agents and steps`() {
        val yaml = """
            workflow:
              name: multi-step
              version: 2
              inputs:
                type: object
                required: [repo]
              agents:
                - id: planner
                  backend: langgraph
                  model_profile: planning-default
              steps:
                - id: plan
                  type: agent
                  agent: planner
                  input:
                    repo: "${'$'}{inputs.repo}"
                  next: done
                - id: done
                  type: result
                  output:
                    plan: "${'$'}{steps.plan.output}"
        """.trimIndent()

        val definition = WorkflowParser.parse(yaml)
        assertEquals("multi-step", definition.name)
        assertEquals(1, definition.agents.size)
        assertEquals("langgraph", definition.agents[0].backend)
        assertEquals(2, definition.steps.size)
        assertEquals("done", definition.steps[0].next)
    }

    @Test
    fun `parse workflow with retries`() {
        val yaml = """
            workflow:
              name: retry-test
              version: 1
              steps:
                - id: flaky-step
                  type: tool
                  tool: some-tool
                  retries:
                    max_attempts: 5
                    backoff_ms: 3000
                    multiplier: 1.5
                  next: done
                - id: done
                  type: result
        """.trimIndent()

        val definition = WorkflowParser.parse(yaml)
        val retries = definition.steps[0].retries
        assertNotNull(retries)
        assertEquals(5, retries.maxAttempts)
        assertEquals(3000, retries.backoffMs)
        assertEquals(1.5, retries.multiplier)
    }

    @Test
    fun `parse workflow with approval step`() {
        val yaml = """
            workflow:
              name: approval-test
              version: 1
              steps:
                - id: review
                  type: approval
                  approvers: [team-lead, manager]
                  timeout: 86400
                  next: done
                - id: done
                  type: result
        """.trimIndent()

        val definition = WorkflowParser.parse(yaml)
        val approvalStep = definition.steps[0]
        assertEquals(StepKind.APPROVAL, approvalStep.type)
        assertEquals(listOf("team-lead", "manager"), approvalStep.approvers)
        assertEquals(86400, approvalStep.timeout)
    }

    @Test
    fun `parse rejects missing workflow key`() {
        val yaml = """
            name: bad-workflow
            steps: []
        """.trimIndent()

        assertFailsWith<IllegalArgumentException> {
            WorkflowParser.parse(yaml)
        }
    }

    @Test
    fun `parse rejects missing steps`() {
        val yaml = """
            workflow:
              name: no-steps
              version: 1
        """.trimIndent()

        assertFailsWith<IllegalArgumentException> {
            WorkflowParser.parse(yaml)
        }
    }

    @Test
    fun `parse workflow with policies`() {
        val yaml = """
            workflow:
              name: policy-test
              version: 1
              policies:
                - id: strict
                  inline:
                    allowed_backends: [langgraph]
                    allowed_tools: [github-read]
                    timeout_seconds: 300
                    require_approval: true
              steps:
                - id: done
                  type: result
        """.trimIndent()

        val definition = WorkflowParser.parse(yaml)
        assertEquals(1, definition.policies.size)
        val policy = definition.policies[0].inline
        assertNotNull(policy)
        assertEquals(listOf("langgraph"), policy.allowedBackends)
        assertEquals(true, policy.requireApproval)
    }
}
```

### 2. PolicyGuard Tests

**Module**: `runtime/policy`
**Test location**: `runtime/policy/src/test/kotlin/org/pekkoagent/policy/`

```kotlin
package org.pekora.policy

import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import kotlin.test.assertTrue
import kotlin.test.assertFalse
import kotlin.test.assertEquals

class PolicyGuardTest {

    @Test
    fun `allows step with no policy restrictions`() {
        val guard = PolicyGuard()
        val step = StepDefinition(id = "test", type = StepKind.AGENT, agent = "planner")
        val agents = mapOf("planner" to AgentDefinition(id = "planner", backend = "langgraph"))

        val decision = guard.evaluate(step, agents, emptyList())
        assertTrue(decision.allowed)
        assertTrue(decision.violations.isEmpty())
    }

    @Test
    fun `blocks step with disallowed backend`() {
        val policy = PolicyDefinition(
            id = "strict",
            allowedBackends = listOf("langgraph"),
        )
        val guard = PolicyGuard(globalPolicies = listOf(policy))
        val step = StepDefinition(id = "test", type = StepKind.AGENT, agent = "coder")
        val agents = mapOf("coder" to AgentDefinition(id = "coder", backend = "strands"))

        val decision = guard.evaluate(step, agents, emptyList())
        assertFalse(decision.allowed)
        assertEquals(1, decision.violations.size)
        assertTrue(decision.violations[0].contains("strands"))
    }

    @Test
    fun `blocks disallowed tool`() {
        val policy = PolicyDefinition(
            id = "tool-policy",
            allowedTools = listOf("github-read"),
        )
        val guard = PolicyGuard(globalPolicies = listOf(policy))
        val step = StepDefinition(id = "test", type = StepKind.TOOL, tool = "dangerous-tool")

        val decision = guard.evaluate(step, emptyMap(), emptyList())
        assertFalse(decision.allowed)
    }

    @Test
    fun `allows permitted tool`() {
        val policy = PolicyDefinition(
            id = "tool-policy",
            allowedTools = listOf("github-read", "test-runner"),
        )
        val guard = PolicyGuard(globalPolicies = listOf(policy))
        val step = StepDefinition(id = "test", type = StepKind.TOOL, tool = "github-read")

        val decision = guard.evaluate(step, emptyMap(), emptyList())
        assertTrue(decision.allowed)
    }

    @Test
    fun `detects approval requirement`() {
        val policy = PolicyDefinition(id = "approval-required", requireApproval = true)
        val guard = PolicyGuard(globalPolicies = listOf(policy))
        val step = StepDefinition(id = "test", type = StepKind.AGENT, agent = "planner")

        val decision = guard.evaluate(step, emptyMap(), emptyList())
        assertTrue(decision.requiresApproval)
    }

    @Test
    fun `applies most restrictive timeout`() {
        val policy1 = PolicyDefinition(id = "p1", timeoutSeconds = 600)
        val policy2 = PolicyDefinition(id = "p2", timeoutSeconds = 300)
        val guard = PolicyGuard(globalPolicies = listOf(policy1))
        val step = StepDefinition(id = "test", type = StepKind.AGENT, agent = "x")

        val decision = guard.evaluate(step, emptyMap(), listOf(policy2))
        assertEquals(300, decision.effectiveTimeoutSeconds)
    }

    @Test
    fun `evaluateToolCall blocks disallowed tool`() {
        val policy = PolicyDefinition(id = "p", allowedTools = listOf("safe-tool"))
        val guard = PolicyGuard(globalPolicies = listOf(policy))

        val decision = guard.evaluateToolCall("unsafe-tool", emptyList())
        assertFalse(decision.allowed)
    }

    @Test
    fun `evaluateSkillCall allows permitted skill`() {
        val policy = PolicyDefinition(id = "p", allowedSkills = listOf("coding", "review"))
        val guard = PolicyGuard(globalPolicies = listOf(policy))

        val decision = guard.evaluateSkillCall("coding", emptyList())
        assertTrue(decision.allowed)
    }
}
```

### 3. RunEntity Tests (Pekko TestKit)

**Module**: `runtime/run-engine`
**Test location**: `runtime/run-engine/src/test/kotlin/org/pekkoagent/engine/`

These tests use the Pekko Persistence TestKit:

```kotlin
package org.pekora.engine

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe
import org.apache.pekko.persistence.typed.PersistenceId
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RunEntityTest {

    companion object {
        private val testKit = ActorTestKit.create()

        @JvmStatic
        @AfterAll
        fun teardown() {
            testKit.shutdownTestKit()
        }
    }

    private fun simpleDefinition() = WorkflowDefinition(
        name = "test-wf",
        version = 1,
        steps = listOf(
            StepDefinition(id = "step1", type = StepKind.RESULT, output = mapOf("msg" to "hello")),
        ),
    )

    @Test
    fun `create run succeeds`() {
        val stepExecutor = testKit.createTestProbe<StepExecutorMessage>()
        val probe = testKit.createTestProbe<RunCommandResponse>()
        val entity = testKit.spawn(
            RunEntity.create("run-1", PersistenceId.ofUniqueId("run-1"), stepExecutor.ref)
        )

        entity.tell(CreateRun(
            templateId = "tmpl-1",
            versionNumber = 1,
            inputs = mapOf("repo" to "org/repo"),
            replyTo = probe.ref,
        ))

        val response = probe.receiveMessage()
        assertTrue(response.success)
    }

    @Test
    fun `load workflow and start run`() {
        val stepExecutor = testKit.createTestProbe<StepExecutorMessage>()
        val probe = testKit.createTestProbe<RunCommandResponse>()
        val entity = testKit.spawn(
            RunEntity.create("run-2", PersistenceId.ofUniqueId("run-2"), stepExecutor.ref)
        )

        // Create
        entity.tell(CreateRun("tmpl-1", 1, mapOf("repo" to "org/repo"), replyTo = probe.ref))
        probe.receiveMessage()

        // Load workflow
        entity.tell(LoadWorkflow(simpleDefinition(), probe.ref))
        probe.receiveMessage()

        // Start
        entity.tell(StartRun(probe.ref))
        val startResp = probe.receiveMessage()
        assertTrue(startResp.success)
    }

    @Test
    fun `get run status`() {
        val stepExecutor = testKit.createTestProbe<StepExecutorMessage>()
        val cmdProbe = testKit.createTestProbe<RunCommandResponse>()
        val statusProbe = testKit.createTestProbe<RunStatusResponse>()
        val entity = testKit.spawn(
            RunEntity.create("run-3", PersistenceId.ofUniqueId("run-3"), stepExecutor.ref)
        )

        // Create
        entity.tell(CreateRun("tmpl-1", 1, emptyMap(), replyTo = cmdProbe.ref))
        cmdProbe.receiveMessage()

        // Check status
        entity.tell(GetRunStatus(statusProbe.ref))
        val status = statusProbe.receiveMessage()
        assertEquals("run-3", status.runId)
        assertEquals(RunState.LOADING_DEFINITION, status.status)
    }

    @Test
    fun `cancel run`() {
        val stepExecutor = testKit.createTestProbe<StepExecutorMessage>()
        val probe = testKit.createTestProbe<RunCommandResponse>()
        val entity = testKit.spawn(
            RunEntity.create("run-4", PersistenceId.ofUniqueId("run-4"), stepExecutor.ref)
        )

        entity.tell(CreateRun("tmpl-1", 1, emptyMap(), replyTo = probe.ref))
        probe.receiveMessage()

        entity.tell(CancelRun(reason = "test cancel", replyTo = probe.ref))
        val resp = probe.receiveMessage()
        assertTrue(resp.success)

        // Verify state
        val statusProbe = testKit.createTestProbe<RunStatusResponse>()
        entity.tell(GetRunStatus(statusProbe.ref))
        val status = statusProbe.receiveMessage()
        assertEquals(RunState.CANCELLED, status.status)
    }
}
```

### 4. Projection Tests

**Module**: `runtime/projection`
**Test location**: `runtime/projection/src/test/kotlin/org/pekkoagent/projection/`

```kotlin
package org.pekora.projection

import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class RunProjectionStoreTest {

    @Test
    fun `tracks run lifecycle`() {
        val store = RunProjectionStore()

        store.applyEvent(RunCreated("run-1", "tmpl-1", 1, mapOf("repo" to "test")))
        val summary = store.getSummary("run-1")
        assertNotNull(summary)
        assertEquals(RunState.CREATED, summary.status)

        store.applyEvent(RunStarted("run-1"))
        assertEquals(RunState.EXECUTING, store.getSummary("run-1")!!.status)

        store.applyEvent(StepStarted("run-1", "step1", "langgraph"))
        assertEquals(StepState.RUNNING, store.getSummary("run-1")!!.stepStates["step1"])

        store.applyEvent(StepCompleted("run-1", "step1", mapOf("result" to "ok")))
        assertEquals(StepState.SUCCEEDED, store.getSummary("run-1")!!.stepStates["step1"])

        store.applyEvent(RunCompleted("run-1", mapOf("final" to "output")))
        assertEquals(RunState.COMPLETED, store.getSummary("run-1")!!.status)
    }

    @Test
    fun `builds timeline`() {
        val store = RunProjectionStore()

        store.applyEvent(RunCreated("run-1", "tmpl-1", 1, emptyMap()))
        store.applyEvent(RunStarted("run-1"))
        store.applyEvent(StepStarted("run-1", "step1"))

        val timeline = store.getTimeline("run-1")
        assertEquals(3, timeline.entries.size)
        assertEquals("RunCreated", timeline.entries[0].eventType)
        assertEquals("RunStarted", timeline.entries[1].eventType)
        assertEquals("StepStarted", timeline.entries[2].eventType)
        assertEquals("step1", timeline.entries[2].stepId)
    }

    @Test
    fun `lists active runs`() {
        val store = RunProjectionStore()

        store.applyEvent(RunCreated("run-1", "tmpl-1", 1, emptyMap()))
        store.applyEvent(RunStarted("run-1"))

        store.applyEvent(RunCreated("run-2", "tmpl-1", 1, emptyMap()))
        store.applyEvent(RunStarted("run-2"))
        store.applyEvent(RunCompleted("run-2"))

        val active = store.getActiveRuns()
        assertEquals(1, active.size)
        assertEquals("run-1", active[0].runId)
    }

    @Test
    fun `tracks approval state`() {
        val store = RunProjectionStore()

        store.applyEvent(RunCreated("run-1", "tmpl-1", 1, emptyMap()))
        store.applyEvent(RunStarted("run-1"))
        store.applyEvent(ApprovalRequested("run-1", "review", "appr-1", listOf("lead")))

        assertEquals(RunState.WAITING_FOR_APPROVAL, store.getSummary("run-1")!!.status)
        assertEquals(StepState.BLOCKED, store.getSummary("run-1")!!.stepStates["review"])

        store.applyEvent(ApprovalReceived("run-1", "review", "appr-1", true, "lead"))
        assertEquals(RunState.EXECUTING, store.getSummary("run-1")!!.status)
        assertEquals(StepState.SUCCEEDED, store.getSummary("run-1")!!.stepStates["review"])
    }

    @Test
    fun `filters by tenant`() {
        val store = RunProjectionStore()

        store.applyEvent(RunCreated("run-1", "tmpl-1", 1, emptyMap(), tenantId = "tenant-a"))
        store.applyEvent(RunCreated("run-2", "tmpl-1", 1, emptyMap(), tenantId = "tenant-b"))

        val tenantA = store.listRuns(tenantId = "tenant-a")
        assertEquals(1, tenantA.size)
        assertEquals("run-1", tenantA[0].runId)
    }

    @Test
    fun `returns empty for unknown run`() {
        val store = RunProjectionStore()
        assertNull(store.getSummary("nonexistent"))
        assertEquals(0, store.getTimeline("nonexistent").entries.size)
    }
}
```

---

## Integration Testing with the HTTP API

### Start the Server

```bash
./gradlew :runtime:api:run
```

### Register a Workflow Template

```bash
curl -X POST http://localhost:8080/workflow-templates \
  -H "Content-Type: application/json" \
  -d '{
    "id": "issue-to-pr",
    "name": "Issue to PR Workflow",
    "description": "Converts GitHub issues into pull requests",
    "owner": "platform-team"
  }'
```

### Publish a Workflow Version

```bash
curl -X POST http://localhost:8080/workflow-templates/issue-to-pr/versions \
  -H "Content-Type: application/json" \
  -d '{
    "version": 1,
    "workflowYaml": "workflow:\n  name: issue-to-pr\n  version: 1\n  steps:\n    - id: start\n      type: result\n      output:\n        status: done"
  }'
```

### List Templates

```bash
curl http://localhost:8080/workflow-templates
```

### Start a Run

```bash
curl -X POST http://localhost:8080/runs \
  -H "Content-Type: application/json" \
  -d '{
    "templateId": "issue-to-pr",
    "version": 1,
    "inputs": {
      "repo": "myorg/myrepo",
      "issue_id": "42"
    }
  }'
```

### Check Run Status

```bash
curl http://localhost:8080/runs/{runId}
```

### Cancel a Run

```bash
curl -X POST http://localhost:8080/runs/{runId}/cancel
```

### List Pending Approvals

```bash
curl http://localhost:8080/approvals
```

### Approve a Checkpoint

```bash
curl -X POST http://localhost:8080/approvals/{approvalId}/approve \
  -H "Content-Type: application/json" \
  -d '{
    "approver": "team-lead",
    "reason": "Changes look good"
  }'
```

### Reject a Checkpoint

```bash
curl -X POST http://localhost:8080/approvals/{approvalId}/reject \
  -H "Content-Type: application/json" \
  -d '{
    "approver": "tech-lead",
    "reason": "Needs more test coverage"
  }'
```

---

## Testing the Example Workflow

The `examples/issue-to-pr/workflow.yaml` file exercises the full DSL. You can parse it as a unit test:

```kotlin
@Test
fun `parse issue-to-pr example`() {
    val yaml = File("../../examples/issue-to-pr/workflow.yaml").readText()
    val definition = WorkflowParser.parse(yaml)

    assertEquals("issue-to-pr", definition.name)
    assertEquals(4, definition.agents.size)
    assertEquals(3, definition.tools.size)
    assertEquals(8, definition.steps.size)

    // Verify step chain
    val stepIds = definition.steps.map { it.id }
    assertTrue("classify" in stepIds)
    assertTrue("approve" in stepIds)
    assertTrue("done" in stepIds)

    // Verify approval step
    val approvalStep = definition.steps.find { it.id == "approve" }!!
    assertEquals(StepKind.APPROVAL, approvalStep.type)
    assertEquals(listOf("team-lead", "tech-lead"), approvalStep.approvers)
}
```

---

## Testing Adapters

### Mock Adapter for Unit Tests

Create test doubles for adapter interfaces:

```kotlin
class MockAgentRuntimeAdapter(
    override val backendId: String = "mock",
    private val result: StepExecutionResult = StepExecutionResult(status = StepResultStatus.SUCCEEDED),
) : AgentRuntimeAdapter {

    val executedSteps = mutableListOf<StepExecutionRequest>()

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        executedSteps.add(request)
        return CompletableFuture.completedFuture(result)
    }
}

class MockToolAdapter(
    override val adapterId: String = "mock-tools",
    private val result: ToolInvocationResult = ToolInvocationResult(status = StepResultStatus.SUCCEEDED),
) : ToolAdapter {

    val invokedTools = mutableListOf<ToolInvocationRequest>()

    override fun invoke(request: ToolInvocationRequest): CompletionStage<ToolInvocationResult> {
        invokedTools.add(request)
        return CompletableFuture.completedFuture(result)
    }
}
```

### Testing LangGraph Adapter

For integration testing the LangGraph adapter, start a mock HTTP server:

```kotlin
@Test
fun `langgraph adapter handles successful response`() {
    // Start a mock HTTP server on a random port that returns:
    // {"status": "succeeded", "output": {"plan": "do stuff"}}

    val adapter = LangGraphAdapter(serviceUrl = "http://localhost:${mockPort}")
    val request = StepExecutionRequest(
        runId = "run-1",
        stepId = "plan",
        stepKind = StepKind.AGENT,
        backend = "langgraph",
        definitionRef = "planner_graph:v1",
        input = mapOf("repo" to "org/repo"),
    )

    val result = adapter.executeStep(request).toCompletableFuture().get()
    assertEquals(StepResultStatus.SUCCEEDED, result.status)
    assertEquals("do stuff", result.output["plan"])
}

@Test
fun `langgraph adapter handles connection failure`() {
    val adapter = LangGraphAdapter(serviceUrl = "http://localhost:1") // unreachable
    val request = StepExecutionRequest(
        runId = "run-1",
        stepId = "plan",
        stepKind = StepKind.AGENT,
        backend = "langgraph",
    )

    val result = adapter.executeStep(request).toCompletableFuture().get()
    assertEquals(StepResultStatus.FAILED, result.status)
    assertTrue(result.error!!.contains("connection failed", ignoreCase = true))
}
```

---

## Test Configuration

### Pekko TestKit Configuration

For `runtime/run-engine` tests, add this to `src/test/resources/application.conf`:

```hocon
pekko {
  loglevel = "WARNING"

  actor {
    provider = "local"
  }

  persistence {
    journal.plugin = "pekko.persistence.journal.inmem"
    snapshot-store.plugin = "pekko.persistence.snapshot-store.local"
    snapshot-store.local.dir = "target/snapshots"
  }
}
```

This uses the in-memory journal and local snapshot store for fast test execution without external dependencies.

---

## Checklist

When adding new features, ensure:

- [ ] Unit tests for new domain models and parsing
- [ ] PolicyGuard tests for new policy types
- [ ] RunEntity state transition tests for new event types
- [ ] Projection tests for new event handling
- [ ] Adapter mock tests for new adapter implementations
- [ ] Integration tests via HTTP API for new endpoints
- [ ] Example workflow YAML updated if DSL changes
