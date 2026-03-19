package org.pekora.adapters.bedrockagentcore

import com.sun.net.httpserver.HttpServer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.pekora.adapters.HealthStatus
import org.pekora.dsl.*
import java.net.InetSocketAddress
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BedrockAgentCoreAdapterTest {

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
        backend = "bedrock-agentcore",
        definitionRef = "",
        input = mapOf("task" to "analyze"),
        constraints = StepConstraints(timeoutSeconds = 30),
    )

    private fun makeAdapter(oauthToken: String = "test-token") = BedrockAgentCoreAdapter(
        agentRuntimeArn = "arn:aws:bedrock-agentcore:us-east-1:123456789012:agent-runtime/test",
        region = "us-east-1",
        authMode = "oauth",
        oauthToken = oauthToken,
        endpointOverride = "http://localhost:$port",
    )

    @Test
    fun `executeStep returns FAILED when agentRuntimeArn is not configured`() {
        val adapter = BedrockAgentCoreAdapter(agentRuntimeArn = "")
        val result = adapter.executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.FAILED, result.status)
        assertTrue(result.error?.contains("agent-runtime-arn") == true)
    }

    @Test
    fun `executeStep with oauth auth sends Bearer header`() {
        var authHeader: String? = null
        var requestBody: String? = null

        server.createContext("/runtimes") { exchange ->
            authHeader = exchange.requestHeaders.getFirst("Authorization")
            requestBody = exchange.requestBody.bufferedReader().readText()
            val response = """{"response":{"message":"analysis complete"}}"""
            val bytes = response.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val result = makeAdapter(oauthToken = "my-oauth-token").executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("analysis complete", result.output["result"])
        assertEquals("Bearer my-oauth-token", authHeader)
    }

    @Test
    fun `executeStep parses response message into output`() {
        server.createContext("/runtimes") { exchange ->
            val response = """{"response":{"message":"result text here"}}"""
            val bytes = response.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        val result = makeAdapter().executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("result text here", result.output["result"])
    }

    @Test
    fun `executeStep returns FAILED on HTTP error from AgentCore`() {
        server.createContext("/runtimes") { exchange ->
            exchange.sendResponseHeaders(400, 0)
            exchange.close()
        }
        server.start()

        val result = makeAdapter().executeStep(makeRequest()).toCompletableFuture().get()

        assertEquals(StepResultStatus.FAILED, result.status)
        assertTrue(result.error?.contains("400") == true)
    }

    @Test
    fun `executeStep includes runtimeSessionId in request body`() {
        var body: String? = null
        server.createContext("/runtimes") { exchange ->
            body = exchange.requestBody.bufferedReader().readText()
            val response = """{"response":{"message":"ok"}}"""
            val bytes = response.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.write(bytes)
            exchange.close()
        }
        server.start()

        makeAdapter().executeStep(makeRequest()).toCompletableFuture().get()

        val json = Json { ignoreUnknownKeys = true }
        val parsed = json.parseToJsonElement(body!!).jsonObject
        assertTrue(parsed["runtimeSessionId"]?.jsonPrimitive?.content?.contains("run-1") == true)
    }

    @Test
    fun `healthCheck returns UNKNOWN when arn not configured`() {
        val adapter = BedrockAgentCoreAdapter(agentRuntimeArn = "")
        val health = adapter.healthCheck().toCompletableFuture().get()

        assertEquals(HealthStatus.UNKNOWN, health.status)
    }
}
