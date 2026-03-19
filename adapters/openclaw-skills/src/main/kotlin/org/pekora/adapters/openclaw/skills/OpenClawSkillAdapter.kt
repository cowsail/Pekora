/**
 * OpenClaw skill adapter for the Pekko Agent Orchestrator.
 *
 * This file provides an adapter-layer integration with the OpenClaw skill execution service,
 * as described in Section 13 of the design spec (OpenClaw Integration Design). Skills are
 * higher-level capabilities than tools -- a single skill may orchestrate multiple tool calls,
 * prepare execution environments, or apply domain-specific logic before returning a result.
 *
 * The adapter translates [SkillInvocationRequest] objects from the orchestrator's internal DSL
 * into HTTP calls against the OpenClaw REST API, and maps the JSON responses back into
 * [SkillInvocationResult] objects.
 *
 * **Design rule:** Treat OpenClaw compatibility as an adapter layer, not as a core runtime
 * dependency. The orchestrator's core modules never reference OpenClaw types directly; all
 * interaction is mediated through the [SkillAdapter] interface that this class implements.
 *
 * @see org.pekora.adapters.SkillAdapter
 */
package org.pekora.adapters.openclaw.skills

import org.pekora.adapters.SkillAdapter
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
 * Adapter that delegates skill invocations to an OpenClaw-compatible service over HTTP.
 *
 * Each call serialises the orchestrator's [SkillInvocationRequest] into the JSON schema
 * expected by the OpenClaw `/skills/{skillId}/invoke` endpoint, issues an asynchronous
 * HTTP POST, and converts the response (or any transport/parse error) into a
 * [SkillInvocationResult].
 *
 * Skills differ from tools in that they represent higher-level capabilities: a skill may
 * internally coordinate several tool calls, manage temporary environments, or apply
 * composite logic before producing a result.
 *
 * This class is part of the OpenClaw Integration Design described in Section 13 of the
 * design spec. Per the design rule, OpenClaw compatibility is treated as an adapter layer
 * and is not a core runtime dependency -- the orchestrator can function without this
 * adapter present.
 *
 * @property serviceUrl Base URL of the OpenClaw service (e.g. `http://localhost:8200`).
 *   All skill invocation endpoints are resolved relative to this URL.
 * @property httpClient The [HttpClient] used for outbound HTTP requests. Defaults to a
 *   new client created via [HttpClient.newHttpClient]; a custom instance can be injected
 *   for testing or to apply timeouts, proxy settings, etc.
 * @see org.pekora.adapters.SkillAdapter
 */
class OpenClawSkillAdapter(
    private val serviceUrl: String = "http://localhost:8200",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
) : SkillAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(OpenClawSkillAdapter::class.java)
        private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }
    }

    override val adapterId: String = "openclaw-skills"

    /**
     * Invokes an OpenClaw skill by sending an HTTP POST to the OpenClaw service.
     *
     * The method builds a JSON payload containing the skill identifier, input parameters,
     * run identifier, and step identifier from the supplied [request], then posts it to
     * `{serviceUrl}/skills/{skillId}/invoke`. The returned [CompletionStage] completes with
     * a successful [SkillInvocationResult] when the service responds with a 2xx status code,
     * or with a failed result when the service returns an error status or the connection
     * itself fails.
     *
     * @param request The [SkillInvocationRequest] describing which skill to invoke and the
     *   input parameters, run context, and step context for the invocation.
     * @return A [CompletionStage] that completes with a [SkillInvocationResult] representing
     *   either the successful output of the skill or an error description.
     * @see org.pekora.adapters.SkillAdapter.invoke
     */
    override fun invoke(request: SkillInvocationRequest): CompletionStage<SkillInvocationResult> {
        logger.info("OpenClaw skill invocation: ${request.skillId}")

        val payload = json.encodeToString(
            mapOf(
                "skill_id" to request.skillId,
                "input" to request.input.entries.associate { it.key to it.value },
                "run_id" to request.runId,
                "step_id" to request.stepId,
            )
        )

        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/skills/${request.skillId}/invoke"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build()

        return httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() in 200..299) {
                    parseSkillResult(response.body())
                } else {
                    SkillInvocationResult(
                        status = StepResultStatus.FAILED,
                        error = "OpenClaw skill error: HTTP ${response.statusCode()}",
                    )
                }
            }
            .exceptionally { ex ->
                logger.error("OpenClaw skill invocation failed", ex)
                SkillInvocationResult(
                    status = StepResultStatus.FAILED,
                    error = "OpenClaw skill connection failed: ${ex.message}",
                )
            }
    }

    private fun parseSkillResult(body: String): SkillInvocationResult {
        return try {
            val parsed = json.parseToJsonElement(body).jsonObject
            val status = parsed["status"]?.jsonPrimitive?.content
            val outputObj = parsed["output"]?.jsonObject
            val output = outputObj?.entries?.associate { it.key to it.value.jsonPrimitive.content } ?: emptyMap()

            SkillInvocationResult(
                status = when (status) {
                    "succeeded" -> StepResultStatus.SUCCEEDED
                    else -> StepResultStatus.FAILED
                },
                output = output,
                error = parsed["error"]?.jsonPrimitive?.content,
            )
        } catch (e: Exception) {
            SkillInvocationResult(
                status = StepResultStatus.FAILED,
                error = "Failed to parse skill response: ${e.message}",
            )
        }
    }
}
