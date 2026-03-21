package org.pekora.adapters.native

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.pekora.adapters.HealthStatus
import org.pekora.adapters.generic.GenericActorRequest
import org.pekora.dsl.*
import java.time.Duration
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NativeAdapterTest {

    companion object {
        private val testKit = ActorTestKit.create()

        @JvmStatic
        @AfterAll
        fun tearDown() = testKit.shutdownTestKit()
    }

    private fun makeRequest(agentName: String = "echo", runId: String = "run-1") = StepExecutionRequest(
        runId = runId,
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

    // ── SINGLETON scope ───────────────────────────────────────────────────────

    @Test
    fun `SINGLETON dispatches step and returns result`() {
        val registry = NativeAgentRegistry()
        registry.register("echo", PekoraAgentBehavior.create(::EchoAgent), AgentScope.SINGLETON)
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val result = adapter.executeStep(makeRequest("echo")).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("hello", result.output["msg"])
    }

    @Test
    fun `SINGLETON reuses the same actor across multiple runs`() {
        val registry = NativeAgentRegistry()
        registry.register("echo", PekoraAgentBehavior.create(::EchoAgent), AgentScope.SINGLETON)
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val r1 = adapter.executeStep(makeRequest("echo", "run-a")).toCompletableFuture().get()
        val r2 = adapter.executeStep(makeRequest("echo", "run-b")).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, r1.status)
        assertEquals(StepResultStatus.SUCCEEDED, r2.status)
    }

    // ── PER_RUN scope ─────────────────────────────────────────────────────────

    @Test
    fun `PER_RUN dispatches step and returns result`() {
        val registry = NativeAgentRegistry()
        registry.register("echo-run", PekoraAgentBehavior.create(::EchoAgent), AgentScope.PER_RUN)
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val result = adapter.executeStep(makeRequest("echo-run", "run-per-1")).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("hello", result.output["msg"])
    }

    @Test
    fun `cleanupRun stops PER_RUN actors without affecting SINGLETON`() {
        val registry = NativeAgentRegistry()
        registry.register("shared", PekoraAgentBehavior.create(::EchoAgent), AgentScope.SINGLETON)
        registry.register("per-run-agent", PekoraAgentBehavior.create(::EchoAgent), AgentScope.PER_RUN)
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        // Spawn actors by dispatching
        adapter.executeStep(makeRequest("shared", "run-x")).toCompletableFuture().get()
        adapter.executeStep(makeRequest("per-run-agent", "run-x")).toCompletableFuture().get()

        // Cleanup run-x
        adapter.cleanupRun("run-x")

        // SINGLETON still works
        val r = adapter.executeStep(makeRequest("shared", "run-y")).toCompletableFuture().get()
        assertEquals(StepResultStatus.SUCCEEDED, r.status)
    }

    // ── PER_INVOCATION scope ──────────────────────────────────────────────────

    @Test
    fun `PER_INVOCATION sync handler is called and returns result`() {
        val registry = NativeAgentRegistry()
        registry.registerPerInvocation("inv-sync") { request ->
            StepExecutionResult(StepResultStatus.SUCCEEDED, output = mapOf("got" to request.input["msg"]!!))
        }
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val result = adapter.executeStep(makeRequest("inv-sync")).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("hello", result.output["got"])
    }

    @Test
    fun `PER_INVOCATION async handler is called and returns result`() {
        val registry = NativeAgentRegistry()
        registry.registerPerInvocationAsync("inv-async") { request ->
            CompletableFuture.supplyAsync {
                StepExecutionResult(StepResultStatus.SUCCEEDED, output = mapOf("async" to "true"))
            }
        }
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val result = adapter.executeStep(makeRequest("inv-async")).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("true", result.output["async"])
    }

    @Test
    fun `PER_INVOCATION multiple runs dispatch concurrently without blocking each other`() {
        val registry = NativeAgentRegistry()
        val callCount = java.util.concurrent.atomic.AtomicInteger(0)
        registry.registerPerInvocationAsync("concurrent") { _ ->
            CompletableFuture.supplyAsync {
                callCount.incrementAndGet()
                StepExecutionResult(StepResultStatus.SUCCEEDED)
            }
        }
        val adapter = NativeAdapter(registry, testKit.system(), Duration.ofSeconds(5))

        val f1 = adapter.executeStep(makeRequest("concurrent", "run-c1")).toCompletableFuture()
        val f2 = adapter.executeStep(makeRequest("concurrent", "run-c2")).toCompletableFuture()
        val f3 = adapter.executeStep(makeRequest("concurrent", "run-c3")).toCompletableFuture()

        CompletableFuture.allOf(f1, f2, f3).get()
        assertEquals(3, callCount.get())
    }

    // ── Error cases ───────────────────────────────────────────────────────────

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
}
