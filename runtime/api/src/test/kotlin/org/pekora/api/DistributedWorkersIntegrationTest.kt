package org.pekora.api

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.cluster.typed.Join
import org.apache.pekko.persistence.typed.PersistenceId
import org.junit.jupiter.api.Test
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dsl.RunState
import org.pekora.dsl.StepDefinition
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepKind
import org.pekora.dsl.StepResultStatus
import org.pekora.dsl.WorkflowDefinition
import org.pekora.engine.ApprovalManager
import org.pekora.engine.GetRunStatus
import org.pekora.engine.LoadWorkflow
import org.pekora.engine.RunCommand
import org.pekora.engine.RunCommandResponse
import org.pekora.engine.RunEntity
import org.pekora.engine.RunEntityTypeKey
import org.pekora.engine.RunStatusResponse
import org.pekora.engine.StartRun
import org.pekora.engine.StepExecutor
import org.pekora.policy.PolicyGuard
import org.pekora.registry.RegistryCommand
import org.pekora.registry.WorkflowRegistry
import org.pekora.engine.CreateRun
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DistributedWorkersIntegrationTest {

    @Test
    fun `queued execution completes run through embedded worker`() {
        val config = ConfigFactory.parseString(
            """
            pekko.actor.provider = cluster
            pekko.remote.artery.canonical.hostname = "127.0.0.1"
            pekko.remote.artery.canonical.port = 0
            pekko.cluster.seed-nodes = []
            pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
            pekko.persistence.snapshot-store.plugin = "pekko.persistence.snapshot-store.local"
            pekora.distributedWorkers {
              enabled = true
              provider = pekko
              leaseTimeoutMs = 30000
              embeddedWorkers {
                enabled = true
                replicas = 2
                pollIntervalMs = 50
                maxClaimsPerPoll = 4
                workerIdPrefix = "test-worker"
              }
            }
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
            val adapters = mapOf("langgraph" to SuccessAdapter())
            val distributedWorkers = DistributedWorkersSettings.fromConfig(system.settings().config())
            val workDispatch = WorkDispatchFactory.bootstrap(system, distributedWorkers)

            val stepExecutor = testKit.spawn(
                StepExecutor.create(
                    agentAdapters = adapters,
                    policyGuard = PolicyGuard(),
                    stepDispatchGateway = workDispatch.stepDispatchGateway,
                ),
                "step-executor-${UUID.randomUUID()}",
            )

            sharding.init(
                Entity.of(RunEntityTypeKey.typeKey) { entityContext ->
                    RunEntity.create(
                        runId = entityContext.entityId,
                        persistenceId = PersistenceId.of(entityContext.entityTypeKey.name(), entityContext.entityId),
                        stepExecutor = stepExecutor,
                        approvalManager = approvalManager,
                        registry = registry,
                        sharding = sharding,
                    )
                }
            )

            val workers = WorkDispatchFactory.spawnEmbeddedWorkers(
                system = system,
                settings = distributedWorkers,
                workQueueProvider = workDispatch.workQueueProvider,
                agentAdapters = adapters,
                sharding = sharding,
            )
            assertEquals(2, workers.size)

            val runId = "run-queued-${UUID.randomUUID()}"
            val runRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
            val commandProbe = testKit.createTestProbe<RunCommandResponse>()

            val definition = WorkflowDefinition(
                name = "queued-parent",
                version = 1,
                steps = listOf(
                    StepDefinition(id = "agent", type = StepKind.AGENT, agent = "agent-1", next = "done"),
                    StepDefinition(id = "done", type = StepKind.RESULT, output = mapOf("result" to "${'$'}{steps.agent.output.answer}")),
                ),
                agents = listOf(org.pekora.dsl.AgentDefinition(id = "agent-1", backend = "langgraph")),
            )

            runRef.tell(CreateRun("tmpl", 1, emptyMap(), replyTo = commandProbe.ref))
            assertTrue(commandProbe.receiveMessage().success)
            runRef.tell(LoadWorkflow(definition, commandProbe.ref))
            assertTrue(commandProbe.receiveMessage().success)
            runRef.tell(StartRun(commandProbe.ref))
            assertTrue(commandProbe.receiveMessage().success)

            val completed = awaitStatus(testKit, runRef) { status -> status.status == RunState.COMPLETED }
            assertEquals("worker-ok", completed.outputs["result"])
            assertEquals(RunState.COMPLETED, completed.status)
        } finally {
            testKit.shutdownTestKit()
        }
    }

    private fun awaitStatus(
        testKit: ActorTestKit,
        entity: org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef<RunCommand>,
        timeout: Duration = Duration.ofSeconds(10),
        predicate: (RunStatusResponse) -> Boolean,
    ): RunStatusResponse {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            val probe: TestProbe<RunStatusResponse> = testKit.createTestProbe()
            entity.tell(GetRunStatus(probe.ref))
            try {
                val status = probe.receiveMessage(Duration.ofMillis(250))
                if (predicate(status)) {
                    return status
                }
            } catch (_: AssertionError) {
                // retry until timeout
            }
        }
        throw AssertionError("Timed out waiting for run status")
    }
}

private class SuccessAdapter : AgentRuntimeAdapter {
    override val backendId: String = "langgraph"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
        CompletableFuture.completedFuture(
            StepExecutionResult(
                status = StepResultStatus.SUCCEEDED,
                output = mapOf("answer" to "worker-ok"),
            )
        )
}
