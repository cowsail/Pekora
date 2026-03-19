/**
 * A2A (Agent-to-Agent) protocol adapter for the Pekora framework.
 *
 * Implements the A2A open interop standard using JSON-RPC 2.0 over HTTP:
 * - Method: `message/send` for single-turn step execution.
 * - Agent discovery via `GET /.well-known/agent-card.json`.
 *
 * Auth: `Authorization: Bearer <apiKey>` when [apiKey] is non-empty.
 *
 * Phase 2 scope: synchronous `message/send` only. Streaming (`message/stream`) and
 * multi-turn context reuse are deferred to Phase 3.
 *
 * @see org.pekora.adapters.AgentRuntimeAdapter
 */
package org.pekora.adapters.a2a

import kotlinx.serialization.json.*
import org.pekora.adapters.AdapterHealth
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.HealthStatus
import org.pekora.adapters.ValidationResult
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Adapter that dispatches workflow steps to an A2A-compliant agent service.
 *
 * @property serviceUrl Base URL of the A2A agent service (e.g., `"http://localhost:8200"`).
 * @property apiKey Bearer token for authentication. Empty string skips the header.
 * @property httpClient JDK HTTP client used for all outbound requests.
 */
class A2AAdapter(
    private val serviceUrl: String = "http://localhost:8200",
    private val apiKey: String = "",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
) : AgentRuntimeAdapter {

    override val backendId = "a2a"

    companion object {
        private val logger = LoggerFactory.getLogger(A2AAdapter::class.java)
        private val json = Json { ignoreUnknownKeys = true }
    }

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        val inputText = request.input.entries.joinToString("\n") { (k, v) -> "$k: $v" }

        val rpcPayload = buildJsonObject {
            put("jsonrpc", "2.0")
            put("id", request.correlationId)
            put("method", "message/send")
            put("params", buildJsonObject {
                put("message", buildJsonObject {
                    put("role", "user")
                    put("parts", buildJsonArray {
                        add(buildJsonObject {
                            put("kind", "text")
                            put("text", inputText)
                        })
                    })
                    put("messageId", request.stepId)
                })
            })
        }.toString()

        val httpRequest = buildRequest(rpcPayload)

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() !in 200..299) {
                    logger.error("A2A service returned HTTP ${response.statusCode()}: ${response.body()}")
                    return@thenApply StepExecutionResult(
                        status = StepResultStatus.FAILED,
                        error = "A2A service error: HTTP ${response.statusCode()}",
                    )
                }
                parseRpcResponse(response.body(), request.stepId)
            }
            .exceptionally { ex ->
                logger.error("A2A step ${request.stepId} failed", ex)
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "A2A execution failed: ${ex.message}",
                )
            }
    }

    override fun validateDefinition(definition: Map<String, String>): CompletionStage<ValidationResult> {
        val req = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/.well-known/agent-card.json"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .apply { if (apiKey.isNotEmpty()) header("Authorization", "Bearer $apiKey") }
            .build()

        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() in 200..299) {
                    ValidationResult(valid = true)
                } else {
                    ValidationResult(
                        valid = false,
                        errors = listOf("Agent card not found: HTTP ${response.statusCode()}"),
                    )
                }
            }
            .exceptionally { ex ->
                ValidationResult(valid = false, errors = listOf("Agent card fetch failed: ${ex.message}"))
            }
    }

    override fun healthCheck(): CompletionStage<AdapterHealth> {
        val start = System.currentTimeMillis()
        val req = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/.well-known/agent-card.json"))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .apply { if (apiKey.isNotEmpty()) header("Authorization", "Bearer $apiKey") }
            .build()
        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                val latency = System.currentTimeMillis() - start
                if (response.statusCode() in 200..299) {
                    AdapterHealth(backendId, HealthStatus.HEALTHY, latencyMs = latency)
                } else {
                    AdapterHealth(backendId, HealthStatus.UNHEALTHY, "HTTP ${response.statusCode()}", latency)
                }
            }
            .exceptionally { ex ->
                AdapterHealth(backendId, HealthStatus.UNHEALTHY, ex.message ?: "connection failed")
            }
    }

    private fun parseRpcResponse(body: String, stepId: String): StepExecutionResult {
        return try {
            val root = json.parseToJsonElement(body).jsonObject

            // JSON-RPC error
            root["error"]?.jsonObject?.let { error ->
                val message = error["message"]?.jsonPrimitive?.content ?: "Unknown RPC error"
                return StepExecutionResult(status = StepResultStatus.FAILED, error = message)
            }

            val result = root["result"]?.jsonObject
                ?: return StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "A2A response missing result field",
                )

            // Extract text from artifacts
            val outputParts = result["artifacts"]?.jsonArray?.flatMap { artifact ->
                artifact.jsonObject["parts"]?.jsonArray
                    ?.mapNotNull { part ->
                        part.jsonObject.takeIf { it["kind"]?.jsonPrimitive?.content == "text" }
                            ?.get("text")?.jsonPrimitive?.content
                    } ?: emptyList()
            } ?: emptyList()

            val outputText = outputParts.joinToString("\n")
            val output = if (outputText.isNotEmpty()) mapOf("result" to outputText) else emptyMap()

            // Check task status if present
            val taskStatus = result["status"]?.jsonObject?.get("state")?.jsonPrimitive?.content
            if (taskStatus != null && taskStatus !in setOf("completed", "succeeded")) {
                return StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "A2A task ended with status: $taskStatus",
                )
            }

            StepExecutionResult(
                status = StepResultStatus.SUCCEEDED,
                output = output,
            )
        } catch (e: Exception) {
            logger.error("Failed to parse A2A response for step $stepId", e)
            StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "Failed to parse A2A response: ${e.message}",
            )
        }
    }

    private fun buildRequest(body: String): HttpRequest =
        HttpRequest.newBuilder()
            .uri(URI.create(serviceUrl))
            .header("Content-Type", "application/json")
            .apply { if (apiKey.isNotEmpty()) header("Authorization", "Bearer $apiKey") }
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
}
