/**
 * LangGraph AgentServer adapter for the Pekora framework.
 *
 * Implements the LangGraph AgentServer thread + run model:
 * 1. `POST /threads` — create a new thread.
 * 2. `POST /threads/{thread_id}/runs` — create a run with the step input.
 * 3. Poll `GET /threads/{thread_id}/runs/{run_id}` until the run reaches a terminal state.
 * 4. `GET /threads/{thread_id}` — retrieve final thread state on success.
 *
 * Auth: `X-Api-Key` header on all requests when [apiKey] is non-empty.
 *
 * Phase 2 scope: synchronous polling only (no SSE streaming). One thread per step.
 *
 * @see org.pekora.adapters.AgentRuntimeAdapter
 */
package org.pekora.adapters.langgraph

import kotlinx.serialization.json.*
import org.pekora.adapters.AdapterHealth
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.HealthStatus
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.pekora.dsl.ToolCallRecord
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.TimeUnit

/**
 * Adapter that dispatches workflow steps to a LangGraph AgentServer instance.
 *
 * @property serviceUrl Base URL of the LangGraph AgentServer (e.g., `"http://localhost:8123"`).
 * @property apiKey API key sent as `X-Api-Key`. Empty string skips the header.
 * @property httpClient JDK HTTP client used for all outbound requests.
 */
class LangGraphAdapter(
    override val backendId: String = "langgraph",
    private val serviceUrl: String = "http://localhost:8123",
    private val apiKey: String = "",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
) : AgentRuntimeAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(LangGraphAdapter::class.java)
        private val json = Json { ignoreUnknownKeys = true }
        private val TERMINAL_STATUSES = setOf("success", "failure", "timeout", "error", "interrupted")
        private const val INITIAL_POLL_DELAY_MS = 500L
        private const val POLL_BACKOFF_MULTIPLIER = 1.5
        private const val MAX_POLL_DELAY_MS = 5000L
    }

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        val timeoutMs = request.constraints.timeoutSeconds * 1000L
        val timeoutAt = System.currentTimeMillis() + timeoutMs

        return createThread()
            .thenCompose { threadId ->
                createRun(threadId, request).thenCompose { runId ->
                    pollRunStatus(threadId, runId, INITIAL_POLL_DELAY_MS, timeoutAt)
                        .thenCompose { runObj ->
                            val status = runObj["status"]?.jsonPrimitive?.content
                            when {
                                status == "success" ->
                                    fetchThreadState(threadId).thenApply { state -> mapSuccessResult(state) }
                                status == "timeout" || System.currentTimeMillis() > timeoutAt ->
                                    CompletableFuture.completedFuture(
                                        StepExecutionResult(
                                            status = StepResultStatus.TIMED_OUT,
                                            error = "Step timed out after ${request.constraints.timeoutSeconds}s",
                                        )
                                    )
                                else ->
                                    CompletableFuture.completedFuture(
                                        StepExecutionResult(
                                            status = StepResultStatus.FAILED,
                                            error = runObj["error"]?.jsonPrimitive?.content
                                                ?: "Run ended with status: $status",
                                        )
                                    )
                            }
                        }
                }
            }
            .exceptionally { ex ->
                logger.error("LangGraph step ${request.stepId} failed", ex)
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "LangGraph execution failed: ${ex.message}",
                )
            }
    }

    override fun cancelStep(executionId: String): CompletionStage<Unit> {
        val parts = executionId.split("/")
        if (parts.size != 2) return CompletableFuture.completedFuture(Unit)
        val (threadId, runId) = parts
        val req = buildRequest("POST", "$serviceUrl/threads/$threadId/runs/$runId/cancel", "")
        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
            .thenApply { }
            .exceptionally { ex ->
                logger.warn("Failed to cancel LangGraph run $executionId", ex)
            }
    }

    override fun healthCheck(): CompletionStage<AdapterHealth> {
        val start = System.currentTimeMillis()
        val req = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/ok"))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .apply { if (apiKey.isNotEmpty()) header("X-Api-Key", apiKey) }
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

    private fun createThread(): CompletableFuture<String> {
        val req = buildRequest("POST", "$serviceUrl/threads", "{}")
        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() !in 200..299) {
                    throw RuntimeException("Failed to create LangGraph thread: HTTP ${response.statusCode()}")
                }
                val body = json.parseToJsonElement(response.body()).jsonObject
                body["thread_id"]?.jsonPrimitive?.content
                    ?: throw RuntimeException("LangGraph thread response missing thread_id")
            }
    }

    private fun createRun(threadId: String, request: StepExecutionRequest): CompletableFuture<String> {
        val payload = buildJsonObject {
            put("assistant_id", request.definitionRef.ifEmpty { "default" })
            put("input", buildJsonObject {
                request.input.forEach { (k, v) -> put(k, v) }
            })
        }.toString()

        val req = buildRequest("POST", "$serviceUrl/threads/$threadId/runs", payload)
        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() !in 200..299) {
                    throw RuntimeException("Failed to create LangGraph run: HTTP ${response.statusCode()}")
                }
                val body = json.parseToJsonElement(response.body()).jsonObject
                body["run_id"]?.jsonPrimitive?.content
                    ?: throw RuntimeException("LangGraph run response missing run_id")
            }
    }

    private fun pollRunStatus(
        threadId: String,
        runId: String,
        delayMs: Long,
        timeoutAt: Long,
    ): CompletableFuture<JsonObject> {
        if (System.currentTimeMillis() > timeoutAt) {
            return CompletableFuture.completedFuture(buildJsonObject { put("status", "timeout") })
        }
        val executor = CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)
        return CompletableFuture.supplyAsync({}, executor).thenCompose {
            val req = HttpRequest.newBuilder()
                .uri(URI.create("$serviceUrl/threads/$threadId/runs/$runId"))
                .GET()
                .apply { if (apiKey.isNotEmpty()) header("X-Api-Key", apiKey) }
                .build()
            httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
        }.thenCompose { response ->
            val runObj = json.parseToJsonElement(response.body()).jsonObject
            val status = runObj["status"]?.jsonPrimitive?.content
            if (status in TERMINAL_STATUSES) {
                CompletableFuture.completedFuture(runObj)
            } else {
                val nextDelay = minOf((delayMs * POLL_BACKOFF_MULTIPLIER).toLong(), MAX_POLL_DELAY_MS)
                pollRunStatus(threadId, runId, nextDelay, timeoutAt)
            }
        }
    }

    private fun fetchThreadState(threadId: String): CompletableFuture<JsonObject> {
        val req = HttpRequest.newBuilder()
            .uri(URI.create("$serviceUrl/threads/$threadId"))
            .GET()
            .apply { if (apiKey.isNotEmpty()) header("X-Api-Key", apiKey) }
            .build()
        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() !in 200..299) {
                    throw RuntimeException("Failed to fetch thread state: HTTP ${response.statusCode()}")
                }
                json.parseToJsonElement(response.body()).jsonObject
            }
    }

    private fun mapSuccessResult(state: JsonObject): StepExecutionResult {
        val valuesObj = state["values"]?.jsonObject
        val output = valuesObj?.entries?.mapNotNull { (k, v) ->
            try { k to v.jsonPrimitive.content } catch (_: Exception) { null }
        }?.toMap() ?: emptyMap()

        val toolCalls = state["tool_calls"]?.jsonArray?.mapNotNull { tc ->
            try {
                val obj = tc.jsonObject
                val inputMap = obj["input"]?.jsonObject?.entries
                    ?.mapNotNull { (k, v) -> try { k to v.jsonPrimitive.content } catch (_: Exception) { null } }
                    ?.toMap() ?: emptyMap()
                val outputMap = obj["output"]?.jsonObject?.entries
                    ?.mapNotNull { (k, v) -> try { k to v.jsonPrimitive.content } catch (_: Exception) { null } }
                    ?.toMap() ?: obj["output"]?.let { mapOf("result" to it.toString()) } ?: emptyMap()
                ToolCallRecord(
                    tool = obj["name"]?.jsonPrimitive?.content ?: "",
                    input = inputMap,
                    output = outputMap,
                )
            } catch (_: Exception) { null }
        } ?: emptyList()

        return StepExecutionResult(
            status = StepResultStatus.SUCCEEDED,
            output = output,
            toolCalls = toolCalls,
        )
    }

    private fun buildRequest(method: String, url: String, body: String): HttpRequest =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .apply { if (apiKey.isNotEmpty()) header("X-Api-Key", apiKey) }
            .method(method, HttpRequest.BodyPublishers.ofString(body))
            .build()
}
