package org.pekora.adapters.generic

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.pekora.adapters.HealthStatus
import org.pekora.dsl.*
import java.net.InetSocketAddress
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class GenericAdapterTest {

    private lateinit var server: HttpServer
    private var port = 0

    @BeforeEach
    fun setUp() {
        server = HttpServer.create(InetSocketAddress(0), 0)
        port = server.address.port
    }

    @AfterEach
    fun tearDown() {
        server.stop(0)
    }

    private fun makeRequest() = StepExecutionRequest(
        runId = "run-1",
        stepId = "step-1",
        stepKind = StepKind.AGENT,
        backend = "generic",
        definitionRef = "",
        input = mapOf("prompt" to "do something"),
        constraints = StepConstraints(timeoutSeconds = 10),
    )

    @Test
    fun `HTTP mode executes step and parses succeeded response`() {
        server.createContext("/execute") { exchange ->
            val response = """{"status":"succeeded","output":{"result":"done"}}"""
            val bytes = response.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = GenericAdapter.http("generic", "http://localhost:$port")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("done", result.output["result"])
    }

    @Test
    fun `HTTP mode sends Bearer auth header when apiKey configured`() {
        var authHeader: String? = null

        server.createContext("/execute") { exchange ->
            authHeader = exchange.requestHeaders.getFirst("Authorization")
            val bytes = """{"status":"succeeded","output":{}}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = GenericAdapter.http("generic", "http://localhost:$port", apiKey = "my-api-key")
        adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals("Bearer my-api-key", authHeader)
    }

    @Test
    fun `HTTP mode returns FAILED on HTTP error`() {
        server.createContext("/execute") { exchange ->
            exchange.sendResponseHeaders(500, 0)
            exchange.close()
        }
        server.start()

        val adapter = GenericAdapter.http("generic", "http://localhost:$port")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.FAILED, result.status)
        assertTrue(result.error?.contains("500") == true)
    }

    @Test
    fun `HTTP mode healthCheck returns HEALTHY when health endpoint is 200`() {
        server.createContext("/health") { exchange ->
            val bytes = "ok".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = GenericAdapter.http("generic", "http://localhost:$port")
        val health = adapter.healthCheck().toCompletableFuture().get()

        assertEquals(HealthStatus.HEALTHY, health.status)
    }

    @Test
    fun `HTTP mode healthCheck returns UNHEALTHY when health endpoint fails`() {
        server.createContext("/health") { exchange ->
            exchange.sendResponseHeaders(503, 0)
            exchange.close()
        }
        server.start()

        val adapter = GenericAdapter.http("generic", "http://localhost:$port")
        val health = adapter.healthCheck().toCompletableFuture().get()

        assertEquals(HealthStatus.UNHEALTHY, health.status)
    }
}
