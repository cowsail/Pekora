package org.pekora.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.cluster.typed.Join
import org.apache.pekko.http.javadsl.Http
import org.apache.pekko.http.javadsl.ServerBinding
import org.apache.pekko.persistence.typed.PersistenceId
import org.junit.jupiter.api.Test
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dsl.AgentDefinition
import org.pekora.dsl.RunState
import org.pekora.dsl.StepDefinition
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepKind
import org.pekora.dsl.StepResultStatus
import org.pekora.dsl.WorkflowDefinition
import org.pekora.engine.ApprovalManager
import org.pekora.engine.CreateRun
import org.pekora.engine.GetRunStatus
import org.pekora.engine.LoadWorkflow
import org.pekora.engine.RunCommandResponse
import org.pekora.engine.RunEntity
import org.pekora.engine.RunEntityTypeKey
import org.pekora.engine.RunStatusResponse
import org.pekora.engine.StartRun
import org.pekora.engine.StepExecutor
import org.pekora.framework.DistributedWorkersSettings
import org.pekora.framework.WorkDispatchFactory
import org.pekora.policy.PolicyGuard
import org.pekora.projection.DefaultRunEventProjector
import org.pekora.projection.InMemoryRunNotificationStore
import org.pekora.projection.InMemoryRunProjectionStore
import org.pekora.registry.WorkflowRegistry
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RunRoutesQueryIntegrationTest {

    @Test
    fun `run query endpoints expose summaries timeline and step outputs`() {
        TestHarness(QuerySuccessAdapter()).use { harness ->
            val runId = harness.createAndStartRun(tenantId = "tenant-a")
            harness.awaitStatus(runId) { it.status == RunState.COMPLETED }

            val runs = harness.getJson("/runs")
            assertTrue(runs.any { it["runId"].asText() == runId })

            val tenantRuns = harness.getJson("/runs?tenantId=tenant-a")
            assertEquals(listOf(runId), tenantRuns.map { it["runId"].asText() })

            val activeRuns = harness.getJson("/runs/active")
            assertEquals(0, activeRuns.size())

            val timeline = harness.getJson("/runs/$runId/timeline")
            assertEquals(runId, timeline["runId"].asText())
            assertTrue(timeline["entries"].any { it["eventType"].asText() == "RunCompleted" })

            val stepOutput = harness.getJson("/runs/$runId/steps/done/output")
            assertEquals(runId, stepOutput["runId"].asText())
            assertEquals("done", stepOutput["stepId"].asText())
            assertEquals("worker-ok", stepOutput["output"]["result"].asText())
        }
    }

    @Test
    fun `active runs and missing query resources behave as expected`() {
        TestHarness(QueryHangingAdapter()).use { harness ->
            val runId = harness.createAndStartRun(tenantId = "tenant-b")
            harness.awaitStatus(runId) { it.status == RunState.EXECUTING }

            val activeRuns = harness.getJson("/runs/active?tenantId=tenant-b")
            assertEquals(listOf(runId), activeRuns.map { it["runId"].asText() })

            val missingTimeline = harness.get("/runs/does-not-exist/timeline")
            assertEquals(404, missingTimeline.statusCode())

            val missingStepOutput = harness.get("/runs/$runId/steps/unknown/output")
            assertEquals(404, missingStepOutput.statusCode())
        }
    }

    @Test
    fun `run events endpoint streams snapshots and progress updates`() {
        TestHarness(QuerySuccessAdapter()).use { harness ->
            val runId = harness.createLoadedRun(tenantId = "tenant-sse")
            harness.openEventStream("/runs/$runId/events").use { stream ->
                val started = harness.startRun(runId)
                assertTrue(started.success)

                val snapshot = stream.nextEvent()
                assertEquals("snapshot", snapshot["event"])
                assertTrue(snapshot["data"]!!.contains("\"runId\":\"$runId\""))

                val updates = mutableListOf<Map<String, String>>()
                while (updates.size < 6) {
                    val event = stream.nextEvent()
                    updates.add(event)
                    if (event["data"]!!.contains("\"eventType\":\"RunCompleted\"")) {
                        break
                    }
                }

                assertTrue(updates.any { it["id"] != null })
                assertTrue(updates.any { it["data"]!!.contains("\"eventType\":\"RunStarted\"") })
                assertTrue(updates.any { it["data"]!!.contains("\"eventType\":\"RunCompleted\"") })
            }
        }
    }

    @Test
    fun `run events endpoint replays from last event id`() {
        TestHarness(QuerySuccessAdapter()).use { harness ->
            val runId = harness.createLoadedRun(tenantId = "tenant-sse-replay")
            var runStartedId: String? = null

            harness.openEventStream("/runs/$runId/events").use { stream ->
                assertTrue(harness.startRun(runId).success)

                stream.nextEvent()
                while (true) {
                    val event = stream.nextEvent()
                    val data = event["data"] ?: ""
                    if (data.contains("\"eventType\":\"RunStarted\"")) {
                        runStartedId = event["id"]
                    }
                    if (data.contains("\"eventType\":\"RunCompleted\"")) {
                        break
                    }
                }
            }

            val replayFrom = runStartedId ?: throw AssertionError("Expected to capture RunStarted event id")
            harness.openEventStream("/runs/$runId/events", replayFrom).use { stream ->
                val replayed = mutableListOf<Map<String, String>>()
                while (replayed.size < 6) {
                    val event = stream.nextEvent()
                    replayed.add(event)
                    if (event["data"]!!.contains("\"eventType\":\"RunCompleted\"")) {
                        break
                    }
                }

                assertTrue(replayed.none { it["event"] == "snapshot" })
                assertTrue(replayed.any { it["data"]!!.contains("\"eventType\":\"RunCompleted\"") })
                assertTrue(replayed.all { it["id"]!!.toLong() > replayFrom.toLong() })
            }
        }
    }
}

private class TestHarness(
    private val adapter: AgentRuntimeAdapter,
) : AutoCloseable {
    private val mapper = jacksonObjectMapper()
    private val client = HttpClient.newHttpClient()
    private val testKit: ActorTestKit
    private val sharding: ClusterSharding
    private val binding: ServerBinding
    private val port: Int
    private val runNotifications = InMemoryRunNotificationStore()

    init {
        val config = ConfigFactory.parseString(
            """
            pekko.actor.provider = cluster
            pekko.remote.artery.canonical.hostname = "127.0.0.1"
            pekko.remote.artery.canonical.port = 0
            pekko.cluster.seed-nodes = []
            pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
            pekko.persistence.snapshot-store.plugin = "pekko.persistence.snapshot-store.local"
            pekora.distributedWorkers.enabled = false
            """.trimIndent()
        ).withFallback(ConfigFactory.load())

        testKit = ActorTestKit.create(config)
        val system = testKit.system()
        val cluster = Cluster.get(system)
        cluster.manager().tell(Join.create(cluster.selfMember().address()))
        Thread.sleep(500)

        sharding = ClusterSharding.get(system)
        val registry = testKit.spawn(WorkflowRegistry.create(), "workflow-registry-${UUID.randomUUID()}")
        val approvalManager = testKit.spawn(ApprovalManager.create(), "approval-manager-${UUID.randomUUID()}")
        val runProjection = InMemoryRunProjectionStore()
        val runProjector = DefaultRunEventProjector(runProjection, runNotifications)
        val distributedWorkers = DistributedWorkersSettings.fromConfig(system.settings().config())
        val workDispatch = WorkDispatchFactory.bootstrap(system, distributedWorkers)
        val stepExecutor = testKit.spawn(
            StepExecutor.create(
                agentAdapters = mapOf(adapter.backendId to adapter),
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
                    eventObserver = runProjector::project,
                )
            }
        )

        val route = RunRoutes(sharding, registry, approvalManager, runProjection, runNotifications, system).routes()
        binding = Http.get(system).newServerAt("127.0.0.1", 0).bind(route).toCompletableFuture().get()
        port = binding.localAddress().port
    }

    fun createAndStartRun(tenantId: String): String {
        val runId = createLoadedRun(tenantId)
        val started = startRun(runId)
        assertTrue(started.success)
        return runId
    }

    fun createLoadedRun(tenantId: String): String {
        val runId = "run-${UUID.randomUUID()}"
        val runRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
        val probe = testKit.createTestProbe<RunCommandResponse>()
        runRef.tell(CreateRun("tmpl", 1, emptyMap(), tenantId = tenantId, replyTo = probe.ref))
        assertTrue(probe.receiveMessage().success)
        runRef.tell(LoadWorkflow(definition(), probe.ref))
        assertTrue(probe.receiveMessage().success)
        return runId
    }

    fun startRun(runId: String): RunCommandResponse {
        val runRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
        val probe = testKit.createTestProbe<RunCommandResponse>()
        runRef.tell(StartRun(probe.ref))
        return probe.receiveMessage()
    }

    fun awaitStatus(
        runId: String,
        timeout: Duration = Duration.ofSeconds(10),
        predicate: (RunStatusResponse) -> Boolean,
    ): RunStatusResponse {
        val entity = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
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

    fun get(path: String): HttpResponse<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:$port$path"))
            .GET()
            .build()
        return client.send(request, HttpResponse.BodyHandlers.ofString())
    }

    fun getJson(path: String): JsonNode {
        val response = get(path)
        assertEquals(200, response.statusCode(), response.body())
        return mapper.readTree(response.body())
    }

    fun openEventStream(path: String, lastEventId: String? = null): SseStream {
        val connection = URI.create("http://127.0.0.1:$port$path").toURL().openConnection()
        connection.connectTimeout = 5_000
        connection.readTimeout = 5_000
        connection.setRequestProperty("Accept", "text/event-stream")
        if (lastEventId != null) {
            connection.setRequestProperty("Last-Event-ID", lastEventId)
        }
        connection.connect()
        val reader = BufferedReader(InputStreamReader(connection.getInputStream()))
        return SseStream(reader)
    }

    override fun close() {
        binding.unbind().toCompletableFuture().get()
        testKit.shutdownTestKit()
    }

    private fun definition(): WorkflowDefinition =
        WorkflowDefinition(
            name = "queryable-run",
            version = 1,
            steps = listOf(
                StepDefinition(id = "agent", type = StepKind.AGENT, agent = "agent-1", next = "done"),
                StepDefinition(id = "done", type = StepKind.RESULT, output = mapOf("result" to "${'$'}{steps.agent.output.answer}")),
            ),
            agents = listOf(AgentDefinition(id = "agent-1", backend = adapter.backendId)),
        )
}

private class SseStream(
    private val reader: BufferedReader,
) : AutoCloseable {
    fun nextEvent(): Map<String, String> {
        val event = linkedMapOf<String, String>()
        val dataLines = mutableListOf<String>()
        while (true) {
            val line = reader.readLine() ?: throw AssertionError("SSE stream closed unexpectedly")
            if (line.isEmpty()) {
                if (dataLines.isNotEmpty()) {
                    event["data"] = dataLines.joinToString("\n")
                }
                if (event.isNotEmpty()) {
                    return event
                }
                continue
            }

            val delimiter = line.indexOf(':')
            if (delimiter <= 0) {
                continue
            }

            val field = line.substring(0, delimiter)
            val value = line.substring(delimiter + 1).trimStart()
            if (field == "data") {
                dataLines.add(value)
            } else {
                event[field] = value
            }
        }
    }

    override fun close() {
        reader.close()
    }
}

private class QuerySuccessAdapter : AgentRuntimeAdapter {
    override val backendId: String = "langgraph"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
        CompletableFuture.completedFuture(
            StepExecutionResult(
                status = StepResultStatus.SUCCEEDED,
                output = mapOf("answer" to "worker-ok"),
            )
        )
}

private class QueryHangingAdapter : AgentRuntimeAdapter {
    override val backendId: String = "langgraph"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
        CompletableFuture()
}
