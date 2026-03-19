/**
 * OpenClaw tool adapter for the Pekko Agent Orchestrator.
 *
 * This file provides an adapter-layer integration with the OpenClaw tool execution service,
 * as described in Section 13 of the design spec (OpenClaw Integration Design). It translates
 * [ToolInvocationRequest] objects from the orchestrator's internal DSL into HTTP calls against
 * the OpenClaw REST API, and maps the JSON responses back into [ToolInvocationResult] objects.
 *
 * **Design rule:** Treat OpenClaw compatibility as an adapter layer, not as a core runtime
 * dependency. The orchestrator's core modules never reference OpenClaw types directly; all
 * interaction is mediated through the [ToolAdapter] interface that this class implements.
 *
 * @see org.pekora.adapters.ToolAdapter
 */
package org.pekora.adapters.openclaw.tools

import org.pekora.adapters.ToolAdapter
import org.pekora.dsl.*
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
 * Adapter that delegates tool invocations to an OpenClaw-compatible service over HTTP.
 *
 * Each call serialises the orchestrator's [ToolInvocationRequest] into the JSON schema
 * expected by the OpenClaw `/tools/{toolId}/invoke` endpoint, issues an asynchronous
 * HTTP POST, and converts the response (or any transport/parse error) into a
 * [ToolInvocationResult].
 *
 * This class is part of the OpenClaw Integration Design described in Section 13 of the
 * design spec. Per the design rule, OpenClaw compatibility is treated as an adapter layer
 * and is not a core runtime dependency -- the orchestrator can function without this
 * adapter present.
 *
 * @property serviceUrl Base URL of the OpenClaw service (e.g. `http://localhost:8200`).
 *   All tool invocation endpoints are resolved relative to this URL.
 * @property httpClient The [HttpClient] used for outbound HTTP requests. Defaults to a
 *   new client created via [HttpClient.newHttpClient]; a custom instance can be injected
 *   for testing or to apply timeouts, proxy settings, etc.
 * @see org.pekora.adapters.ToolAdapter
 */
class OpenClawToolAdapter(
    private val serviceUrl: String = "http://localhost:8200",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
) : ToolAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(OpenClawToolAdapter::class.java)
        private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }
    }

    override val adapterId: String = "openclaw-tools"

    /**
     * Invokes an OpenClaw tool by sending an HTTP POST to the OpenClaw service.
     *
     * The method builds a JSON payload containing the tool identifier, input parameters,
     * run identifier, and step identifier from the supplied [request], then posts it to
     * `{serviceUrl}/tools/{toolId}/invoke`. The returned [CompletionStage] completes with
     * a successful [ToolInvocationResult] when the service responds with a 2xx status code,
     * or with a failed result when the service returns an error status or the connection
     * itself fails.
     *
     * @param request The [ToolInvocationRequest] describing which tool to invoke and the
     *   input parameters, run context, and step context for the invocation.
     * @return A [CompletionStage] that completes with a [ToolInvocationResult] representing
     *   either the successful output of the tool or an error description.
     * @see org.pekora.adapters.ToolAdapter.invoke
     */
    override fun invoke(request: ToolInvocationRequest): CompletionStage<ToolInvocationResult> {
        logger.info("OpenClaw tool invocation: ${request.toolId}")

        val payload = json.encodeToString(
            mapOf(
                "tool_id" to request.toolId,
                "input" to request.input.entries.associate { it.key to it.value },
                "run_id" to request.runId,
                "step_id" to request.stepId,
            )
        )

        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/tools/${request.toolId}/invoke"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build()

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() in 200..299) {
                    parseToolResult(response.body())
                } else {
                    ToolInvocationResult(
                        status = StepResultStatus.FAILED,
                        error = "OpenClaw tool error: HTTP ${response.statusCode()}",
                    )
                }
            }
            .exceptionally { ex ->
                logger.error("OpenClaw tool invocation failed", ex)
                ToolInvocationResult(
                    status = StepResultStatus.FAILED,
                    error = "OpenClaw tool connection failed: ${ex.message}",
                )
            }
    }

    private fun parseToolResult(body: String): ToolInvocationResult {
        return try {
            val parsed = json.parseToJsonElement(body).jsonObject
            val status = parsed["status"]?.jsonPrimitive?.content
            val outputObj = parsed["output"]?.jsonObject
            val output = outputObj?.entries?.associate { it.key to it.value.jsonPrimitive.content } ?: emptyMap()

            ToolInvocationResult(
                status = when (status) {
                    "succeeded" -> StepResultStatus.SUCCEEDED
                    else -> StepResultStatus.FAILED
                },
                output = output,
                error = parsed["error"]?.jsonPrimitive?.content,
            )
        } catch (e: Exception) {
            ToolInvocationResult(
                status = StepResultStatus.FAILED,
                error = "Failed to parse tool response: ${e.message}",
            )
        }
    }
}
