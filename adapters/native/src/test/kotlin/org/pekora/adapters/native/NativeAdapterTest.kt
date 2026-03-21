package org.pekora.adapters.native

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.pekora.adapters.HealthStatus
import org.pekora.adapters.generic.GenericActorRequest
import org.pekora.dsl.*
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NativeAdapterTest {

    companion object {
        private val testKit = ActorTestKit.create()

        @JvmStatic
        @AfterAll
        fun tearDown() = testKit.shutdownTestKit()
    }

    private fun makeRequest(agentName: String = "echo") = StepExecutionRequest(
        runId = "run-1",
        stepId = "step-1",
        stepKind = StepKind.AGENT,
        backend = "native",
        definitionRef = agentName,
        input = mapOf("msg" to "hello"),
    )

    private class EchoAgent(context: ActorContext<GenericActorRequest>) : PekoraAgentBehavior(context) {
        override fun handleStep(request: StepExecutionRequest) =
            StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = request.input)
    }

    @Test
    fun `dispatches step to registered native agent and returns result`() {
        val registry = NativeAgentRegistry()
        registry.register("echo", PekoraAgentBehavior.create(::EchoAgent))

        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))
        val result = adapter.executeStep(makeRequest("echo")).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("hello", result.output["msg"])
    }

    @Test
    fun `returns FAILED when agent name is not registered`() {
        val registry = NativeAgentRegistry()
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val result = adapter.executeStep(makeRequest("unknown")).toCompletableFuture().get()

        assertEquals(StepResultStatus.FAILED, result.status)
        assertTrue(result.error?.contains("unknown") == true)
    }

    @Test
    fun `health check is UNKNOWN when no agents registered`() {
        val registry = NativeAgentRegistry()
        val adapter = NativeAdapter(registry, testKit.system())

        val health = adapter.healthCheck().toCompletableFuture().get()
        assertEquals(HealthStatus.UNKNOWN, health.status)
    }

    @Test
    fun `health check is HEALTHY when agents are registered`() {
        val registry = NativeAgentRegistry()
        registry.register("echo", PekoraAgentBehavior.create(::EchoAgent))
        val adapter = NativeAdapter(registry, testKit.system())

        val health = adapter.healthCheck().toCompletableFuture().get()
        assertEquals(HealthStatus.HEALTHY, health.status)
        assertTrue(health.message.contains("echo"))
    }

    @Test
    fun `registry spawns actor lazily and caches for subsequent dispatches`() {
        val registry = NativeAgentRegistry()
        registry.register("echo", PekoraAgentBehavior.create(::EchoAgent))
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val r1 = adapter.executeStep(makeRequest("echo")).toCompletableFuture().get()
        val r2 = adapter.executeStep(makeRequest("echo")).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, r1.status)
        assertEquals(StepResultStatus.SUCCEEDED, r2.status)
    }
}
