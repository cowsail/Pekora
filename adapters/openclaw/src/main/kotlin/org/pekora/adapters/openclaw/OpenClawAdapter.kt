/**
 * OpenClaw adapter for the Pekka Agent Workflow Framework.
 *
 * Dispatches step execution to an OpenClaw agent runtime via HTTP.
 * This replaces the previous split between openclaw-tools and openclaw-skills adapters.
 * All step types are now handled through the unified agent execution endpoint.
 *
 * @see org.pekora.adapters.AgentRuntimeAdapter
 */
package org.pekora.adapters.openclaw

import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletionStage
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Adapter that executes workflow steps via an OpenClaw agent runtime service.
 *
 * POSTs each [StepExecutionRequest] to `/agents/{agentId}/execute` and normalises the
 * response into a [StepExecutionResult].
 *
 * @property serviceUrl Base URL of the OpenClaw service. Defaults to `http://localhost:8200`.
 * @property httpClient HTTP client for outbound requests. Injectable for testing.
 */
class OpenClawAdapter(
    private val serviceUrl: String = "http://localhost:8200",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
) : AgentRuntimeAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(OpenClawAdapter::class.java)
        private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }
    }

    override val backendId: String = "openclaw"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        logger.info("OpenClaw executing step: ${request.stepId} (ref=${request.definitionRef})")

        val payload = json.encodeToString(
            mapOf(
                "run_id" to request.runId,
                "step_id" to request.stepId,
                "agent_ref" to request.definitionRef,
                "input" to request.input.entries.associate { it.key to it.value },
                "context" to request.context.entries.associate { it.key to it.value },
            )
        )

        val agentId = request.definitionRef.ifBlank { "default" }
        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/agents/$agentId/execute"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build()

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() in 200..299) {
                    parseResult(response.body())
                } else {
                    logger.error("OpenClaw service returned ${response.statusCode()}: ${response.body()}")
                    StepExecutionResult(
                        status = StepResultStatus.FAILED,
                        error = "OpenClaw service error: HTTP ${response.statusCode()}",
                    )
                }
            }
            .exceptionally { ex ->
                logger.error("OpenClaw execution failed", ex)
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "OpenClaw connection failed: ${ex.message}",
                )
            }
    }

    private fun parseResult(body: String): StepExecutionResult {
        return try {
            val parsed = json.parseToJsonElement(body).jsonObject
            val status = parsed["status"]?.jsonPrimitive?.content
            val outputObj = parsed["output"]?.jsonObject
            val output = outputObj?.entries?.associate { it.key to it.value.jsonPrimitive.content } ?: emptyMap()
            val error = parsed["error"]?.jsonPrimitive?.content

            StepExecutionResult(
                status = when (status) {
                    "succeeded" -> StepResultStatus.SUCCEEDED
                    "failed" -> StepResultStatus.FAILED
                    "cancelled" -> StepResultStatus.CANCELLED
                    else -> StepResultStatus.FAILED
                },
                output = output,
                error = error,
            )
        } catch (e: Exception) {
            logger.error("Failed to parse OpenClaw response", e)
            StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "Failed to parse OpenClaw response: ${e.message}",
            )
        }
    }
}
