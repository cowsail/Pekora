package org.pekora.adapters.a2a

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.pekora.adapters.HealthStatus
import org.pekora.dsl.*
import java.net.InetSocketAddress
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class A2AAdapterTest {

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
        backend = "a2a",
        definitionRef = "my-agent",
        input = mapOf("task" to "summarize"),
        constraints = StepConstraints(timeoutSeconds = 10),
    )

    @Test
    fun `executeStep sends JSON-RPC message-send and extracts artifact text`() {
        val rpcResponse = """
            {
              "jsonrpc": "2.0",
              "id": "corr-1",
              "result": {
                "artifacts": [
                  {"parts": [{"kind": "text", "text": "summary result"}]}
                ],
                "status": {"state": "completed"}
              }
            }
        """.trimIndent()

        server.createContext("/") { exchange ->
            val bytes = rpcResponse.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = A2AAdapter(serviceUrl = "http://localhost:$port")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("summary result", result.output["result"])
    }

    @Test
    fun `executeStep sends Bearer auth header when apiKey configured`() {
        var authHeader: String? = null

        server.createContext("/") { exchange ->
            authHeader = exchange.requestHeaders.getFirst("Authorization")
            val bytes = """{"jsonrpc":"2.0","id":"x","result":{"artifacts":[],"status":{"state":"completed"}}}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = A2AAdapter(serviceUrl = "http://localhost:$port", apiKey = "token-abc")
        adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals("Bearer token-abc", authHeader)
    }

    @Test
    fun `executeStep returns FAILED on JSON-RPC error`() {
        val errorResponse = """
            {
              "jsonrpc": "2.0",
              "id": "x",
              "error": {"code": -32600, "message": "agent unavailable"}
            }
        """.trimIndent()

        server.createContext("/") { exchange ->
            val bytes = errorResponse.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = A2AAdapter(serviceUrl = "http://localhost:$port")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.FAILED, result.status)
        assertTrue(result.error?.contains("agent unavailable") == true)
    }

    @Test
    fun `executeStep returns FAILED on HTTP error status`() {
        server.createContext("/") { exchange ->
            exchange.sendResponseHeaders(503, 0)
            exchange.close()
        }
        server.start()

        val adapter = A2AAdapter(serviceUrl = "http://localhost:$port")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.FAILED, result.status)
    }

    @Test
    fun `healthCheck returns HEALTHY when agent-card responds 200`() {
        server.createContext("/.well-known/agent-card.json") { exchange ->
            val bytes = """{"name":"test-agent"}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = A2AAdapter(serviceUrl = "http://localhost:$port")
        val health = adapter.healthCheck().toCompletableFuture().get()

        assertEquals(HealthStatus.HEALTHY, health.status)
    }

    @Test
    fun `healthCheck returns UNHEALTHY when agent-card returns 404`() {
        server.createContext("/.well-known/agent-card.json") { exchange ->
            exchange.sendResponseHeaders(404, 0)
            exchange.close()
        }
        server.start()

        val adapter = A2AAdapter(serviceUrl = "http://localhost:$port")
        val health = adapter.healthCheck().toCompletableFuture().get()

        assertEquals(HealthStatus.UNHEALTHY, health.status)
    }
}
