package org.pekora.engine

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.cluster.typed.Join
import org.apache.pekko.persistence.typed.PersistenceId
import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import org.pekora.registry.*
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFalse

class RunEntityIntegrationTest {

    @Test
    fun `parallel step waits for all branches before join`() {
        withHarness { harness ->
            val runId = "run-parallel-${UUID.randomUUID()}"
            val entity = harness.sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
            val commandProbe = harness.testKit.createTestProbe<RunCommandResponse>()

            val parentDefinition = WorkflowDefinition(
                name = "parallel-parent",
                version = 1,
                steps = listOf(
                    StepDefinition(id = "fanout", type = StepKind.PARALLEL, parallel = listOf("a", "b"), joinNext = "join"),
                    StepDefinition(id = "a", type = StepKind.AGENT, next = "join"),
                    StepDefinition(id = "b", type = StepKind.AGENT, next = "join"),
                    StepDefinition(id = "join", type = StepKind.RESULT, output = mapOf("ok" to "true")),
                ),
            )

            createLoadStart(entity, commandProbe, parentDefinition)

            val first = harness.awaitStepExecutorMessage { it is ExecuteStep && it.request.runId == runId }
            val second = harness.awaitStepExecutorMessage { it is ExecuteStep && it.request.runId == runId }
            val branchStepIds = setOf((first as ExecuteStep).request.stepId, (second as ExecuteStep).request.stepId)
            assertEquals(setOf("a", "b"), branchStepIds)

            entity.tell(
                StepResult(
                    stepId = "a",
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("a" to "done")),
                )
            )

            val midStatus = awaitStatus(harness, entity) { status ->
                status.parallelGroups["fanout"]?.pendingBranches?.size == 1
            }
            assertTrue(midStatus.parallelGroups.containsKey("fanout"))

            entity.tell(
                StepResult(
                    stepId = "b",
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("b" to "done")),
                )
            )

            val joinMsg = harness.awaitStepExecutorMessage {
                it is ExecuteResultStep && it.runId == runId && it.stepId == "join"
            } as ExecuteResultStep
            assertEquals("join", joinMsg.stepId)

            entity.tell(
                StepResult(
                    stepId = "join",
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("ok" to "true")),
                )
            )

            val finalStatus = awaitStatus(harness, entity) { it.status == org.pekora.dsl.RunState.COMPLETED }
            assertEquals(org.pekora.dsl.RunState.COMPLETED, finalStatus.status)
        }
    }

    @Test
    fun `subworkflow step retries child run and uses incremented child run id`() {
        withHarness { harness ->
            val childTemplateId = "child-${UUID.randomUUID()}"
            val parentRunId = "run-subwf-${UUID.randomUUID()}"

            val childDefinition = WorkflowDefinition(
                name = "child",
                version = 1,
                steps = listOf(
                    StepDefinition(id = "child_agent", type = StepKind.AGENT, next = "done"),
                    StepDefinition(id = "done", type = StepKind.RESULT, output = mapOf("child" to "ok")),
                ),
            )
            publishTemplateVersion(harness, childTemplateId, 1, childDefinition)

            val parentDefinition = WorkflowDefinition(
                name = "parent",
                version = 1,
                steps = listOf(
                    StepDefinition(
                        id = "sw",
                        type = StepKind.SUBWORKFLOW,
                        subworkflow = childTemplateId,
                        subworkflowVersion = 1,
                        retries = RetryConfig(maxAttempts = 2, backoffMs = 1, multiplier = 1.0),
                        next = "done",
                    ),
                    StepDefinition(id = "done", type = StepKind.RESULT, output = mapOf("result" to "${'$'}{steps.sw.output}")),
                ),
            )

            val parentEntity = harness.sharding.entityRefFor(RunEntityTypeKey.typeKey, parentRunId)
            val commandProbe = harness.testKit.createTestProbe<RunCommandResponse>()
            createLoadStart(parentEntity, commandProbe, parentDefinition)

            val childRun1 = (harness.awaitStepExecutorMessage {
                it is ExecuteStep && it.request.stepId == "child_agent" && it.request.runId.startsWith("${parentRunId}__sw__")
            } as ExecuteStep).request.runId
            assertTrue(childRun1.endsWith("__sw__1"))

            val childEntity1 = harness.sharding.entityRefFor(RunEntityTypeKey.typeKey, childRun1)
            childEntity1.tell(
                StepResult(
                    stepId = "child_agent",
                    result = StepExecutionResult(status = StepResultStatus.FAILED, error = "boom"),
                )
            )

            val childRun2 = (harness.awaitStepExecutorMessage {
                it is ExecuteStep && it.request.stepId == "child_agent" && it.request.runId.startsWith("${parentRunId}__sw__") && it.request.runId != childRun1
            } as ExecuteStep).request.runId
            assertTrue(childRun2.endsWith("__sw__2"))

            val childEntity2 = harness.sharding.entityRefFor(RunEntityTypeKey.typeKey, childRun2)
            childEntity2.tell(
                StepResult(
                    stepId = "child_agent",
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("phase" to "agent-ok")),
                )
            )

            val childDone = harness.awaitStepExecutorMessage {
                it is ExecuteResultStep && it.runId == childRun2 && it.stepId == "done"
            } as ExecuteResultStep
            childEntity2.tell(
                StepResult(
                    stepId = childDone.stepId,
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("child" to "ok")),
                )
            )

            val parentDone = harness.awaitStepExecutorMessage {
                it is ExecuteResultStep && it.runId == parentRunId && it.stepId == "done"
            } as ExecuteResultStep
            parentEntity.tell(
                StepResult(
                    stepId = parentDone.stepId,
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("result" to "ok")),
                )
            )

            val parentStatus = awaitStatus(harness, parentEntity) { it.status == org.pekora.dsl.RunState.COMPLETED }
            assertEquals(org.pekora.dsl.RunState.COMPLETED, parentStatus.status)
            assertEquals(childRun2, parentStatus.subworkflowChildren["sw"]?.childRunId)
        }
    }

    @Test
    fun `stale step result from previous attempt is ignored after retry is scheduled`() {
        withHarness { harness ->
            val runId = "run-retry-${UUID.randomUUID()}"
            val entity = harness.sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
            val commandProbe = harness.testKit.createTestProbe<RunCommandResponse>()

            val definition = WorkflowDefinition(
                name = "retry-parent",
                version = 1,
                steps = listOf(
                    StepDefinition(
                        id = "agent",
                        type = StepKind.AGENT,
                        agent = "agent-1",
                        retries = RetryConfig(maxAttempts = 2, backoffMs = 1, multiplier = 1.0),
                        next = "done",
                    ),
                    StepDefinition(id = "done", type = StepKind.RESULT, output = mapOf("result" to "${'$'}{steps.agent.output.answer}")),
                ),
                agents = listOf(AgentDefinition(id = "agent-1", backend = "langgraph")),
            )

            createLoadStart(entity, commandProbe, definition)

            val firstAttempt = harness.awaitStepExecutorMessage {
                it is ExecuteStep && it.request.runId == runId && it.request.stepId == "agent"
            } as ExecuteStep
            assertEquals(1, firstAttempt.attempt)

            entity.tell(
                StepResult(
                    stepId = "agent",
                    attempt = 1,
                    result = StepExecutionResult(status = StepResultStatus.FAILED, error = "boom"),
                )
            )

            val secondAttempt = harness.awaitStepExecutorMessage {
                it is ExecuteStep && it.request.runId == runId && it.request.stepId == "agent" && it.attempt == 2
            } as ExecuteStep
            assertEquals(2, secondAttempt.attempt)

            entity.tell(
                StepResult(
                    stepId = "agent",
                    attempt = 1,
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("answer" to "stale")),
                )
            )

            val staleStatus = awaitStatus(harness, entity) { status ->
                (status.stepStates["agent"] == StepState.RETRYING || status.stepStates["agent"] == StepState.PENDING) &&
                    status.outputs["result"] == null
            }
            assertFalse(staleStatus.status == org.pekora.dsl.RunState.COMPLETED)

            entity.tell(
                StepResult(
                    stepId = "agent",
                    attempt = 2,
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("answer" to "fresh")),
                )
            )

            val done = harness.awaitStepExecutorMessage {
                it is ExecuteResultStep && it.runId == runId && it.stepId == "done"
            } as ExecuteResultStep
            entity.tell(
                StepResult(
                    stepId = done.stepId,
                    attempt = done.attempt,
                    result = StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = mapOf("result" to "fresh")),
                )
            )

            val finalStatus = awaitStatus(harness, entity) { it.status == org.pekora.dsl.RunState.COMPLETED }
            assertEquals("fresh", finalStatus.outputs["result"])
        }
    }

    private fun createLoadStart(
        entity: org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef<RunCommand>,
        probe: TestProbe<RunCommandResponse>,
        definition: WorkflowDefinition,
    ) {
        entity.tell(CreateRun("tmpl", 1, emptyMap(), replyTo = probe.ref))
        val create = probe.receiveMessage()
        assertTrue(create.success, "CreateRun failed: ${create.message}")

        entity.tell(LoadWorkflow(definition, probe.ref))
        val load = probe.receiveMessage()
        assertTrue(load.success, "LoadWorkflow failed: ${load.message}")

        entity.tell(StartRun(probe.ref))
        val start = probe.receiveMessage()
        assertTrue(start.success, "StartRun failed: ${start.message}")
    }

    private fun publishTemplateVersion(
        harness: Harness,
        templateId: String,
        version: Int,
        definition: WorkflowDefinition,
    ) {
        val regProbe = harness.testKit.createTestProbe<RegistryResponse>()
        harness.registry.tell(
            RegisterTemplate(
                id = templateId,
                name = templateId,
                replyTo = regProbe.ref,
            )
        )
        assertTrue(regProbe.receiveMessage().success)

        harness.registry.tell(
            PublishVersion(
                templateId = templateId,
                version = version,
                definition = definition,
                replyTo = regProbe.ref,
            )
        )
        assertTrue(regProbe.receiveMessage().success)
    }

    private fun awaitStatus(
        harness: Harness,
        entity: org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef<RunCommand>,
        timeout: Duration = Duration.ofSeconds(10),
        predicate: (RunStatusResponse) -> Boolean,
    ): RunStatusResponse {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            val remaining = Duration.ofNanos((deadline - System.nanoTime()).coerceAtLeast(1))
            val statusProbe = harness.testKit.createTestProbe<RunStatusResponse>()
            entity.tell(GetRunStatus(statusProbe.ref))
            try {
                val status = statusProbe.receiveMessage(remaining.coerceAtMost(Duration.ofMillis(250)))
                if (predicate(status)) {
                    return status
                }
            } catch (_: AssertionError) {
                // retry until timeout
            }
        }
        throw AssertionError("Timed out waiting for status condition")
    }

    private fun withHarness(block: (Harness) -> Unit) {
        val config = ConfigFactory.parseString(
            """
            pekko.actor.provider = cluster
            pekko.remote.artery.canonical.hostname = "127.0.0.1"
            pekko.remote.artery.canonical.port = 0
            pekko.cluster.seed-nodes = []
            pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
            pekko.persistence.snapshot-store.plugin = "pekko.persistence.snapshot-store.local"
            """.trimIndent()
        ).withFallback(ConfigFactory.load())

        val testKit = ActorTestKit.create(config)
        try {
            val system = testKit.system()
            val cluster = Cluster.get(system)
            cluster.manager().tell(Join.create(cluster.selfMember().address()))
            Thread.sleep(500)
            val sharding = ClusterSharding.get(system)
            val registry = testKit.spawn(WorkflowRegistry.create(), "workflow-registry-${UUID.randomUUID()}")
            val approvalManager = testKit.spawn(ApprovalManager.create(), "approval-manager-${UUID.randomUUID()}")
            val stepExecutorProbe = testKit.createTestProbe<StepExecutorMessage>()

            sharding.init(
                Entity.of(RunEntityTypeKey.typeKey) { entityContext ->
                    RunEntity.create(
                        runId = entityContext.entityId,
                        persistenceId = PersistenceId.of(entityContext.entityTypeKey.name(), entityContext.entityId),
                        stepExecutor = stepExecutorProbe.ref,
                        approvalManager = approvalManager,
                        registry = registry,
                        sharding = sharding,
                    )
                }
            )

            block(Harness(testKit, sharding, registry, stepExecutorProbe))
        } finally {
            testKit.shutdownTestKit()
        }
    }

    private data class Harness(
        val testKit: ActorTestKit,
        val sharding: ClusterSharding,
        val registry: org.apache.pekko.actor.typed.ActorRef<RegistryCommand>,
        val stepExecutorProbe: TestProbe<StepExecutorMessage>,
    ) {
        fun awaitStepExecutorMessage(
            timeout: Duration = Duration.ofSeconds(10),
            predicate: (StepExecutorMessage) -> Boolean,
        ): StepExecutorMessage {
            val deadline = System.nanoTime() + timeout.toNanos()
            var lastError: AssertionError? = null
            while (System.nanoTime() < deadline) {
                val remaining = Duration.ofNanos((deadline - System.nanoTime()).coerceAtLeast(1))
                try {
                    val message = stepExecutorProbe.receiveMessage(remaining)
                    if (predicate(message)) {
                        return message
                    }
                } catch (e: AssertionError) {
                    lastError = e
                }
            }
            throw AssertionError("Timed out waiting for matching StepExecutor message", lastError)
        }
    }
}
