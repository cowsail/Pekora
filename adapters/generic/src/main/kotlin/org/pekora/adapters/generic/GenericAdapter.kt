/**
 * Generic adapter for the Pekka Agent Workflow Framework.
 *
 * Provides two execution modes:
 * - **HTTP mode**: POSTs a [StepExecutionRequest] to a configurable base URL endpoint.
 * - **Actor mode**: Sends the request to a Pekko actor using the ask pattern,
 *   enabling in-process agent logic, human approval flows, and local processing without HTTP.
 *
 * @see org.pekora.adapters.AgentRuntimeAdapter
 */
package org.pekora.adapters.generic

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CompletionStage
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * Message wrapper for actor-mode execution.
 *
 * Actors that handle [GenericAdapter] requests in actor mode must accept this message type.
 * The actor should process [request] and reply to [replyTo] with a [StepExecutionResult].
 *
 * @property request The step execution request to process.
 * @property replyTo The actor reference to reply to with the result.
 */
data class GenericActorRequest(
    val request: StepExecutionRequest,
    val replyTo: ActorRef<StepExecutionResult>,
)

/**
 * Generic adapter that dispatches [StepExecutionRequest] to either an HTTP endpoint or a Pekko actor.
 *
 * Construct via [GenericAdapter.http] or [GenericAdapter.actor].
 *
 * @property backendId Unique backend identifier registered with the [org.pekora.engine.StepExecutor].
 */
class GenericAdapter private constructor(
    override val backendId: String,
    private val mode: Mode,
) : AgentRuntimeAdapter {

    /**
     * Execution mode for the adapter.
     */
    sealed interface Mode {
        /**
         * HTTP mode: serialise the request and POST to [baseUrl]/execute.
         *
         * @property baseUrl Base URL of the remote agent service.
         * @property httpClient HTTP client to use for outbound requests.
         */
        data class Http(
            val baseUrl: String,
            val httpClient: HttpClient = HttpClient.newHttpClient(),
        ) : Mode

        /**
         * Actor mode: send the request to [actorRef] using the Pekko ask pattern.
         *
         * The actor must accept [GenericActorRequest] and reply with [StepExecutionResult].
         *
         * @property actorRef The target actor that processes step execution requests.
         * @property scheduler Pekko scheduler for the ask timeout.
         * @property askTimeout Duration to wait for the actor's reply.
         */
        data class Actor(
            val actorRef: ActorRef<GenericActorRequest>,
            val scheduler: Scheduler,
            val askTimeout: Duration = Duration.ofSeconds(300),
        ) : Mode
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GenericAdapter::class.java)
        private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }

        /**
         * Creates a [GenericAdapter] that dispatches via HTTP.
         *
         * @param backendId The backend identifier (e.g., `"generic-http"`).
         * @param baseUrl Base URL of the remote agent service.
         * @param httpClient Optional custom HTTP client.
         */
        fun http(
            backendId: String,
            baseUrl: String,
            httpClient: HttpClient = HttpClient.newHttpClient(),
        ): GenericAdapter = GenericAdapter(backendId, Mode.Http(baseUrl, httpClient))

        /**
         * Creates a [GenericAdapter] that dispatches to a Pekko actor.
         *
         * The actor must accept [GenericActorRequest] messages and reply with [StepExecutionResult].
         *
         * @param backendId The backend identifier (e.g., `"generic-actor"`).
         * @param actorRef The actor that handles step execution.
         * @param scheduler Pekko scheduler for the ask pattern.
         * @param askTimeout How long to wait for the actor's reply.
         */
        fun actor(
            backendId: String,
            actorRef: ActorRef<GenericActorRequest>,
            scheduler: Scheduler,
            askTimeout: Duration = Duration.ofSeconds(300),
        ): GenericAdapter = GenericAdapter(backendId, Mode.Actor(actorRef, scheduler, askTimeout))
    }

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        return when (val m = mode) {
            is Mode.Http -> executeHttp(request, m)
            is Mode.Actor -> executeActor(request, m)
        }
    }

    private fun executeHttp(request: StepExecutionRequest, mode: Mode.Http): CompletionStage<StepExecutionResult> {
        logger.info("GenericAdapter HTTP executing step: ${request.stepId} (backend=$backendId)")

        val payload = json.encodeToString(request)

        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("${mode.baseUrl}/execute"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(payload))
            .build()

        return mode.httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                if (response.statusCode() in 200..299) {
                    parseResult(response.body())
                } else {
                    logger.error("Generic HTTP agent returned ${response.statusCode()}: ${response.body()}")
                    StepExecutionResult(
                        status = StepResultStatus.FAILED,
                        error = "Agent service error: HTTP ${response.statusCode()}",
                    )
                }
            }
            .exceptionally { ex ->
                logger.error("Generic HTTP execution failed", ex)
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "Agent connection failed: ${ex.message}",
                )
            }
    }

    private fun executeActor(request: StepExecutionRequest, mode: Mode.Actor): CompletionStage<StepExecutionResult> {
        logger.info("GenericAdapter actor executing step: ${request.stepId} (backend=$backendId)")

        return AskPattern.ask(
            mode.actorRef,
            { replyTo: ActorRef<StepExecutionResult> -> GenericActorRequest(request, replyTo) },
            mode.askTimeout,
            mode.scheduler,
        ).exceptionally { ex ->
            logger.error("Generic actor execution failed for step ${request.stepId}", ex)
            StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "Actor execution failed: ${ex.message}",
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
            logger.error("Failed to parse agent response", e)
            StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "Failed to parse agent response: ${e.message}",
            )
        }
    }
}
