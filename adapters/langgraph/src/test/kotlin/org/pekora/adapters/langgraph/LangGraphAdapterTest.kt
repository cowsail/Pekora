package org.pekora.adapters.langgraph

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class LangGraphAdapterTest {

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

    private fun makeRequest(stepId: String = "step-1") = StepExecutionRequest(
        runId = "run-1",
        stepId = stepId,
        stepKind = StepKind.AGENT,
        backend = "langgraph",
        definitionRef = "my-assistant",
        input = mapOf("query" to "hello"),
        constraints = StepConstraints(timeoutSeconds = 10),
    )

    @Test
    fun `executeStep creates thread, run, polls and returns success`() {
        val pollCount = AtomicInteger(0)

        server.createContext("/threads") { exchange ->
            val path = exchange.requestURI.path
            val response = when {
                path == "/threads" && exchange.requestMethod == "POST" ->
                    """{"thread_id":"t-1"}"""
                path == "/threads/t-1/runs" && exchange.requestMethod == "POST" ->
                    """{"run_id":"r-1"}"""
                path == "/threads/t-1/runs/r-1" && exchange.requestMethod == "GET" -> {
                    val count = pollCount.incrementAndGet()
                    if (count < 2) """{"status":"pending"}""" else """{"status":"success"}"""
                }
                path == "/threads/t-1" && exchange.requestMethod == "GET" ->
                    """{"values":{"result":"done","label":"bug"}}"""
                else -> { exchange.sendResponseHeaders(404, 0); exchange.close(); return@createContext }
            }
            val bytes = response.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = LangGraphAdapter(serviceUrl = "http://localhost:$port")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("done", result.output["result"])
        assertEquals("bug", result.output["label"])
    }

    @Test
    fun `executeStep sends X-Api-Key header when configured`() {
        var receivedApiKey: String? = null

        server.createContext("/threads") { exchange ->
            receivedApiKey = exchange.requestHeaders.getFirst("X-Api-Key")
            val response = """{"thread_id":"t-1"}"""
            val bytes = response.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.createContext("/threads/t-1/runs") { exchange ->
            val bytes = """{"run_id":"r-1"}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.createContext("/threads/t-1/runs/r-1") { exchange ->
            val bytes = """{"status":"success"}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.createContext("/threads/t-1") { exchange ->
            val bytes = """{"values":{}}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = LangGraphAdapter(serviceUrl = "http://localhost:$port", apiKey = "my-secret-key")
        adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals("my-secret-key", receivedApiKey)
    }

    @Test
    fun `executeStep returns FAILED when run ends with failure status`() {
        server.createContext("/threads") { exchange ->
            val bytes = """{"thread_id":"t-1"}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.createContext("/threads/t-1/runs") { exchange ->
            val bytes = """{"run_id":"r-1"}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.createContext("/threads/t-1/runs/r-1") { exchange ->
            val bytes = """{"status":"failure","error":"agent crashed"}""".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = LangGraphAdapter(serviceUrl = "http://localhost:$port")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.FAILED, result.status)
        assertTrue(result.error?.contains("agent crashed") == true)
    }

    @Test
    fun `healthCheck returns HEALTHY when service responds 200`() {
        server.createContext("/ok") { exchange ->
            val bytes = "ok".toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val adapter = LangGraphAdapter(serviceUrl = "http://localhost:$port")
        val health = adapter.healthCheck().toCompletableFuture().get()

        assertEquals(org.pekora.adapters.HealthStatus.HEALTHY, health.status)
        assertTrue(health.latencyMs >= 0)
    }
}
