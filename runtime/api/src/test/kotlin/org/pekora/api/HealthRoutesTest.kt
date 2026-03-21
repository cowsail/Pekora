package org.pekora.api

import org.junit.jupiter.api.Test
import org.pekora.adapters.*
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class HealthRoutesTest {

    private fun healthyAdapter(id: String): AgentRuntimeAdapter = object : AgentRuntimeAdapter {
        override val backendId = id
        override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
            CompletableFuture.completedFuture(StepExecutionResult(status = StepResultStatus.SUCCEEDED))
        override fun healthCheck(): CompletionStage<AdapterHealth> =
            CompletableFuture.completedFuture(AdapterHealth(id, HealthStatus.HEALTHY, latencyMs = 5))
    }

    private fun unhealthyAdapter(id: String): AgentRuntimeAdapter = object : AgentRuntimeAdapter {
        override val backendId = id
        override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
            CompletableFuture.completedFuture(StepExecutionResult(status = StepResultStatus.FAILED))
        override fun healthCheck(): CompletionStage<AdapterHealth> =
            CompletableFuture.completedFuture(AdapterHealth(id, HealthStatus.UNHEALTHY, "connection refused"))
    }

    @Test
    fun `health aggregation returns HEALTHY status for healthy adapter`() {
        val adapter = healthyAdapter("langgraph")
        val health = adapter.healthCheck().toCompletableFuture().get()
        assertEquals(HealthStatus.HEALTHY, health.status)
        assertEquals("langgraph", health.backendId)
        assertTrue(health.latencyMs >= 0)
    }

    @Test
    fun `health aggregation returns UNHEALTHY with message for failing adapter`() {
        val adapter = unhealthyAdapter("a2a")
        val health = adapter.healthCheck().toCompletableFuture().get()
        assertEquals(HealthStatus.UNHEALTHY, health.status)
        assertEquals("connection refused", health.message)
    }

    @Test
    fun `AdapterHealth data class holds all fields`() {
        val health = AdapterHealth(
            backendId = "test",
            status = HealthStatus.HEALTHY,
            message = "all good",
            latencyMs = 42,
        )
        assertEquals("test", health.backendId)
        assertEquals(HealthStatus.HEALTHY, health.status)
        assertEquals("all good", health.message)
        assertEquals(42L, health.latencyMs)
    }

    @Test
    fun `default healthCheck returns UNKNOWN`() {
        val adapter = object : AgentRuntimeAdapter {
            override val backendId = "default"
            override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
                CompletableFuture.completedFuture(StepExecutionResult(status = StepResultStatus.SUCCEEDED))
        }
        val health = adapter.healthCheck().toCompletableFuture().get()
        assertEquals(HealthStatus.UNKNOWN, health.status)
    }
}
