/**
 * LangGraph adapter implementation for the Pekko Agent Workflow Framework.
 *
 * This file contains the [LangGraphAdapter], which bridges the Pekko orchestration
 * layer to a remote LangGraph (Python) service over HTTP. As described in Section 12
 * of the design spec, the adapter layer enforces a clean boundary: the Pekko framework
 * retains ownership of workflow state, retry semantics, approval lifecycle, and the
 * canonical event timeline, while the LangGraph service owns graph execution, internal
 * node transitions, and LLM invocations.
 *
 * **Why remote HTTP execution?** LangGraph graphs are defined and executed in Python.
 * Rather than embedding a Python runtime inside the JVM, this adapter communicates with
 * a standalone LangGraph service via a lightweight JSON-over-HTTP protocol. This keeps
 * the deployment topology simple (sidecar or remote service), avoids JVM/Python bridging
 * complexity, and allows the LangGraph service to be scaled, versioned, and monitored
 * independently of the Pekko orchestrator.
 *
 * @see org.pekora.adapters.AgentRuntimeAdapter
 * @see org.pekora.adapters.AdapterInterfaces
 */
package org.pekora.adapters.langgraph

import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.ValidationResult
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.pekora.dsl.ToolCallRecord

/**
 * Adapter that executes workflow steps via a remote LangGraph service (Section 12).
 *
 * This adapter implements the [AgentRuntimeAdapter] contract for the `"langgraph"` backend.
 * It serialises each [StepExecutionRequest] into a JSON payload, POSTs it to the LangGraph
 * service's `/execute` endpoint, and normalises the response into a [StepExecutionResult]
 * that the Pekko framework can consume uniformly.
 *
 * **Boundary between LangGraph and Pekko (Section 12):**
 * - The Pekko framework is responsible for global workflow state, retry/back-off policies,
 *   human-in-the-loop approval gates, version pinning, and the canonical event timeline.
 * - The LangGraph service is responsible for graph execution, internal node routing,
 *   LLM tool-calling loops, and producing a structured output that this adapter maps
 *   back into the framework's normalised result model.
 *
 * This adapter maps framework input/context to LangGraph graph state, normalises returned
 * events, classifies failures (transient HTTP errors vs. permanent graph failures), and
 * validates output conformance.
 *
 * @property serviceUrl Base URL of the LangGraph HTTP service. Defaults to
 *   `http://localhost:8100`, suitable for local development or sidecar deployments.
 * @property httpClient The [HttpClient] used for outbound requests. Injectable for testing
 *   or to share a connection pool across adapters.
 *
 * @see AgentRuntimeAdapter
 * @see org.pekora.engine.StepExecutor
 */
class LangGraphAdapter(
    private val serviceUrl: String = "http://localhost:8100",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
) : AgentRuntimeAdapter {

    companion object {
        /** Logger scoped to the [LangGraphAdapter] class. */
        private val logger = LoggerFactory.getLogger(LangGraphAdapter::class.java)

        /**
         * Shared [Json] instance configured for LangGraph service communication.
         *
         * `ignoreUnknownKeys` is enabled so that the adapter remains forward-compatible
         * when the LangGraph service adds new fields to its response payloads.
         */
        private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }
    }

    /**
     * Backend identifier registered with the framework's step executor.
     *
     * Steps whose `backend` field is set to `"langgraph"` are routed to this adapter.
     *
     * @see AgentRuntimeAdapter.backendId
     */
    override val backendId: String = "langgraph"

    /**
     * Executes a workflow step by delegating to the remote LangGraph service.
     *
     * The method performs the following sequence:
     * 1. Serialises the [StepExecutionRequest] fields (`runId`, `stepId`, `definitionRef`,
     *    `input`, and `context`) into a JSON payload.
     * 2. POSTs the payload to the LangGraph service at `{serviceUrl}/execute`.
     * 3. On a 2xx response, parses the JSON body into a [StepExecutionResult] via [parseResult].
     * 4. On a non-2xx response, returns a [StepResultStatus.FAILED] result with the HTTP status.
     * 5. On a connection or I/O exception, returns a [StepResultStatus.FAILED] result
     *    describing the connectivity failure.
     *
     * The entire call is asynchronous; the returned [CompletionStage] completes on the
     * HTTP client's executor thread.
     *
     * @param request The canonical step execution request containing resolved inputs,
     *   execution context, and the graph reference (`definitionRef`) identifying which
     *   LangGraph graph to invoke.
     * @return A [CompletionStage] that completes with a normalised [StepExecutionResult].
     *   The result will never complete exceptionally; all errors are captured in the
     *   result's [StepExecutionResult.error] field.
     *
     * @see AgentRuntimeAdapter.executeStep
     * @see parseResult
     */
    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        logger.info("LangGraph executing step: ${request.stepId} (ref=${request.definitionRef})")

        val payload = json.encodeToString(
            mapOf(
                "run_id" to request.runId,
                "step_id" to request.stepId,
                "graph_ref" to request.definitionRef,
                "input" to request.input.entries.associate { it.key to it.value },
                "context" to request.context.entries.associate { it.key to it.value },
            )
        )

        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/execute"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build()

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() in 200..299) {
                    parseResult(response.body())
                } else {
                    logger.error("LangGraph service returned ${response.statusCode()}: ${response.body()}")
                    StepExecutionResult(
                        status = StepResultStatus.FAILED,
                        error = "LangGraph service error: HTTP ${response.statusCode()}",
                    )
                }
            }
            .exceptionally { ex ->
                logger.error("LangGraph execution failed", ex)
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "LangGraph connection failed: ${ex.message}",
                )
            }
    }

    /**
     * Cancels a running step execution on the remote LangGraph service.
     *
     * Sends a POST request to the `/cancel/{executionId}` endpoint. The adapter
     * fires-and-forgets the cancellation; it does not inspect the response body.
     * If the service is unreachable the returned [CompletionStage] completes
     * exceptionally, which the framework's retry logic can handle.
     *
     * @param executionId The execution identifier (typically the `stepId` or a
     *   service-assigned correlation ID) of the step to cancel.
     * @return A [CompletionStage] that completes when the cancellation request has
     *   been acknowledged by the LangGraph service.
     *
     * @see AgentRuntimeAdapter.cancelStep
     */
    override fun cancelStep(executionId: String): CompletionStage<Unit> {
        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/cancel/$executionId"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.discarding())
            .thenApply { }
    }

    /**
     * Validates that a step definition is compatible with the LangGraph backend.
     *
     * The current implementation always returns a successful [ValidationResult]. Future
     * versions may query the LangGraph service's `/graphs` endpoint to verify that the
     * referenced graph exists and accepts the declared input schema (Section 12.4).
     *
     * @param definition Backend-specific definition metadata (e.g., graph name, version
     *   constraints) to validate against the LangGraph service.
     * @return A [CompletionStage] containing the [ValidationResult]. Currently always valid.
     *
     * @see AgentRuntimeAdapter.validateDefinition
     */
    override fun validateDefinition(definition: Map<String, String>): CompletionStage<ValidationResult> {
        return CompletableFuture.completedFuture(ValidationResult(valid = true))
    }

    private fun parseResult(body: String): StepExecutionResult {
        return try {
            val parsed = json.parseToJsonElement(body).jsonObject
            val status = parsed["status"]?.jsonPrimitive?.content
            val outputObj = parsed["output"]?.jsonObject
            val output = outputObj?.entries?.associate { it.key to it.value.jsonPrimitive.content } ?: emptyMap()
            val error = parsed["error"]?.jsonPrimitive?.content

            // Parse tool_calls reported by the LangGraph runtime, if present.
            // Pekora does not mediate these calls; they are recorded as-is for audit.
            val toolCalls = parsed["tool_calls"]?.jsonArray?.mapNotNull { element ->
                try {
                    val call = element.jsonObject
                    val tool = call["tool"]?.jsonPrimitive?.content ?: return@mapNotNull null
                    val callInput = call["input"]?.jsonObject
                        ?.entries?.associate { it.key to it.value.jsonPrimitive.content } ?: emptyMap()
                    val callOutput = call["output"]?.jsonObject
                        ?.entries?.associate { it.key to it.value.jsonPrimitive.content } ?: emptyMap()
                    val durationMs = call["duration_ms"]?.jsonPrimitive?.content?.toLongOrNull() ?: 0L
                    ToolCallRecord(tool = tool, input = callInput, output = callOutput, durationMs = durationMs)
                } catch (e: Exception) {
                    logger.warn("Could not parse tool_call entry: ${e.message}")
                    null
                }
            } ?: emptyList()

            StepExecutionResult(
                status = when (status) {
                    "succeeded" -> StepResultStatus.SUCCEEDED
                    "failed" -> StepResultStatus.FAILED
                    "cancelled" -> StepResultStatus.CANCELLED
                    else -> StepResultStatus.FAILED
                },
                output = output,
                toolCalls = toolCalls,
                error = error,
            )
        } catch (e: Exception) {
            logger.error("Failed to parse LangGraph response", e)
            StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "Failed to parse LangGraph response: ${e.message}",
            )
        }
    }
}
