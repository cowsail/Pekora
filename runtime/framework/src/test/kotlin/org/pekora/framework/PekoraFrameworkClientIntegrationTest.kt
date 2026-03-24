package org.pekora.framework

import com.typesafe.config.ConfigFactory
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
import org.pekora.engine.RunStatusResponse
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PekoraFrameworkClientIntegrationTest {

    @Test
    fun `framework can be started without http and driven in process`() {
        val config = ConfigFactory.parseString(
            """
            pekko.actor.provider = cluster
            pekko.remote.artery.canonical.hostname = "127.0.0.1"
            pekko.remote.artery.canonical.port = 0
            pekko.cluster.seed-nodes = []
            pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
            pekko.persistence.snapshot-store.plugin = "pekko.persistence.snapshot-store.local"
            pekora.distributedWorkers.enabled = false
            pekora.adapters.native.enabled = false
            """.trimIndent()
        ).withFallback(ConfigFactory.load())

        val handle = PekoraFramework.start(
            config = config,
            options = PekoraFrameworkOptions(
                adapters = mapOf("inline-test" to SuccessAdapter()),
            ),
        )

        try {
            val client = handle.client()
            val definition = WorkflowDefinition(
                name = "embedded-run",
                version = 1,
                steps = listOf(
                    StepDefinition(id = "agent", type = StepKind.AGENT, agent = "agent-1", next = "done"),
                    StepDefinition(id = "done", type = StepKind.RESULT, output = mapOf("result" to "${'$'}{steps.agent.output.answer}")),
                ),
                agents = listOf(AgentDefinition(id = "agent-1", backend = "inline-test")),
            )

            val createTemplate = client.createTemplate(id = "test-template", name = "Test Template").toCompletableFuture().get()
            assertTrue(createTemplate.success)

            val publishVersion = client.publishVersion("test-template", 1, definition).toCompletableFuture().get()
            assertTrue(publishVersion.success)

            val runId = client.createRun(CreateRunSpec(templateId = "test-template"))
                .toCompletableFuture()
                .get()
                .runId

            val status = awaitRunCompletion(client, runId)
            assertEquals(RunState.COMPLETED, status.status)
            assertEquals("worker-ok", status.outputs["result"])
        } finally {
            handle.system.terminate()
        }
    }

    private fun awaitRunCompletion(
        client: PekoraFrameworkClient,
        runId: String,
        timeout: Duration = Duration.ofSeconds(10),
    ): RunStatusResponse {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            val status = client.getRunStatus(runId).toCompletableFuture().get()
            if (status.status == RunState.COMPLETED) {
                return status
            }
            Thread.sleep(100)
        }
        throw AssertionError("Timed out waiting for run completion")
    }
}

private class SuccessAdapter : AgentRuntimeAdapter {
    override val backendId: String = "inline-test"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
        CompletableFuture.completedFuture(
            StepExecutionResult(
                status = StepResultStatus.SUCCEEDED,
                output = mapOf("answer" to "worker-ok"),
            )
        )
}
