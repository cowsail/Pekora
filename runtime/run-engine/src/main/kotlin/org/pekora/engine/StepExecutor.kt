/**
 * # StepExecutor — Broker for Step Execution Dispatch
 *
 * This file defines [StepExecutor], the actor that implements the Tool Broker and Skill Broker
 * patterns described in Section 6.2 of the architecture. The `StepExecutor` receives step
 * execution requests from [RunEntity] and dispatches them to the appropriate adapter based on
 * the step's kind (AGENT, TOOL, SKILL, or pass-through).
 *
 * ## Broker Pattern
 *
 * Rather than having [RunEntity] interact directly with external runtimes, all step execution
 * is funneled through this broker actor. This provides a single point for:
 * - Adapter lookup and dispatch based on step kind and backend identifier.
 * - Policy enforcement via [PolicyGuard][org.pekora.policy.PolicyGuard].
 * - Centralized error handling and logging.
 *
 * ## Async Handling with pipeToSelf
 *
 * Step adapters return [CompletionStage][java.util.concurrent.CompletionStage] results to
 * support non-blocking I/O (e.g., HTTP calls to external LLM APIs). The `StepExecutor` uses
 * Pekko's `pipeToSelf` pattern to bridge the async `CompletionStage` back into the actor's
 * message-processing loop. When the future completes (successfully or with an exception), a
 * [StepResultInternal] message is created and delivered to the actor, which in turn forwards
 * the result to the originating [RunEntity].
 *
 * ## Message Protocol
 *
 * - [ExecuteStep] — Requests execution of a workflow step via the appropriate adapter.
 * - [ExecuteResultStep] — Handles terminal RESULT steps that simply pass output through.
 * - [StepResultInternal] — Internal message used by `pipeToSelf` to forward async results.
 *
 * @see RunEntity
 * @see org.pekora.adapters.AgentRuntimeAdapter
 * @see org.pekora.adapters.ToolAdapter
 * @see org.pekora.adapters.SkillAdapter
 */
package org.pekora.engine

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.ToolAdapter
import org.pekora.adapters.SkillAdapter
import org.pekora.dsl.*
import org.pekora.policy.PolicyGuard
import org.slf4j.LoggerFactory

/**
 * Sealed interface for all messages accepted by the [StepExecutor] actor.
 *
 * @see ExecuteStep
 * @see ExecuteResultStep
 * @see StepResultInternal
 */
sealed interface StepExecutorMessage

/**
 * Message requesting execution of a workflow step through the appropriate adapter.
 *
 * The [StepExecutor] inspects the [StepExecutionRequest.stepKind] and [StepExecutionRequest.backend]
 * fields to determine which adapter to invoke. The result is asynchronously piped back to the
 * [replyTo] actor as a [StepResult] command.
 *
 * @property request The fully resolved [StepExecutionRequest] containing step kind, backend,
 *                   input parameters, and execution constraints.
 * @property replyTo The [RunEntity] actor reference to receive the [StepResult] upon completion.
 *
 * @see StepExecutor
 * @see StepExecutionRequest
 */
data class ExecuteStep(
    val request: StepExecutionRequest,
    val replyTo: ActorRef<RunCommand>,
) : StepExecutorMessage

/**
 * Message requesting execution of a terminal RESULT step.
 *
 * RESULT steps do not invoke any external adapter; they simply pass the resolved output
 * map back to the [RunEntity] as a successful [StepResult]. This signals the end of the
 * workflow's step chain.
 *
 * @property runId The workflow run identifier (for logging and tracing).
 * @property stepId The identifier of the RESULT step.
 * @property output The resolved output map to return as the step's result.
 * @property replyTo The [RunEntity] actor reference to receive the [StepResult].
 *
 * @see StepExecutor
 */
data class ExecuteResultStep(
    val runId: String,
    val stepId: String,
    val output: Map<String, String>,
    val replyTo: ActorRef<RunCommand>,
) : StepExecutorMessage

/**
 * The step execution broker actor that dispatches workflow steps to the appropriate runtime adapter.
 *
 * `StepExecutor` acts as the ToolBroker and SkillBroker described in Section 6.2 of the
 * architecture. It maintains registries of [AgentRuntimeAdapter], [ToolAdapter], and
 * [SkillAdapter] instances, and routes incoming [ExecuteStep] messages to the correct adapter
 * based on the step's kind and backend configuration.
 *
 * Adapter invocations are asynchronous, returning [CompletionStage][java.util.concurrent.CompletionStage]
 * results. The actor uses Pekko's `pipeToSelf` mechanism to safely deliver these results back
 * into its message loop without blocking the actor thread.
 *
 * @param context The typed actor context for this behavior.
 * @param agentAdapters A map of backend identifiers to [AgentRuntimeAdapter] instances for AGENT steps.
 * @param toolAdapters A map of tool adapter identifiers to [ToolAdapter] instances for TOOL steps.
 * @param skillAdapters A map of skill adapter identifiers to [SkillAdapter] instances for SKILL steps.
 * @param policyGuard The [PolicyGuard] used to enforce execution policies before dispatching steps.
 *
 * @see RunEntity
 * @see StepExecutorMessage
 */
class StepExecutor(
    context: ActorContext<StepExecutorMessage>,
    private val agentAdapters: Map<String, AgentRuntimeAdapter>,
    private val toolAdapters: Map<String, ToolAdapter>,
    private val skillAdapters: Map<String, SkillAdapter>,
    private val policyGuard: PolicyGuard,
) : AbstractBehavior<StepExecutorMessage>(context) {

    /**
     * Companion object providing the factory method and logger for [StepExecutor].
     */
    companion object {
        private val logger = LoggerFactory.getLogger(StepExecutor::class.java)

        /**
         * Factory method that creates a new [StepExecutor] behavior.
         *
         * All adapter maps default to empty, and the [PolicyGuard] defaults to a no-op
         * instance. Callers should supply their own adapter registrations for production use.
         *
         * @param agentAdapters Map of backend identifiers to [AgentRuntimeAdapter] instances. Defaults to empty.
         * @param toolAdapters Map of tool identifiers to [ToolAdapter] instances. Defaults to empty.
         * @param skillAdapters Map of skill identifiers to [SkillAdapter] instances. Defaults to empty.
         * @param policyGuard The [PolicyGuard] for policy enforcement. Defaults to a new [PolicyGuard] instance.
         * @return A [Behavior] that, when materialized, produces a fully initialized [StepExecutor].
         */
        fun create(
            agentAdapters: Map<String, AgentRuntimeAdapter> = emptyMap(),
            toolAdapters: Map<String, ToolAdapter> = emptyMap(),
            skillAdapters: Map<String, SkillAdapter> = emptyMap(),
            policyGuard: PolicyGuard = PolicyGuard(),
        ): Behavior<StepExecutorMessage> = Behaviors.setup { ctx ->
            StepExecutor(ctx, agentAdapters, toolAdapters, skillAdapters, policyGuard)
        }
    }

    /**
     * Constructs the [Receive] handler that routes incoming [StepExecutorMessage] instances
     * to the appropriate handler method.
     *
     * @return A [Receive] instance handling [ExecuteStep] and [ExecuteResultStep] messages.
     */
    override fun createReceive(): Receive<StepExecutorMessage> =
        newReceiveBuilder()
            .onMessage(ExecuteStep::class.java, this::onExecuteStep)
            .onMessage(ExecuteResultStep::class.java, this::onExecuteResultStep)
            .build()

    private fun onExecuteStep(msg: ExecuteStep): Behavior<StepExecutorMessage> {
        val request = msg.request
        logger.info("Executing step ${request.stepId} (kind=${request.stepKind}, backend=${request.backend})")

        context.pipeToSelf(
            executeAsync(request)
        ) { result, throwable ->
            if (throwable != null) {
                logger.error("Step ${request.stepId} failed with exception", throwable)
                val failResult = StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = throwable.message ?: "Unknown error",
                )
                StepResultInternal(request.stepId, failResult, msg.replyTo)
            } else {
                StepResultInternal(request.stepId, result, msg.replyTo)
            }
        }

        return this
    }

    private fun onExecuteResultStep(msg: ExecuteResultStep): Behavior<StepExecutorMessage> {
        val result = StepExecutionResult(
            status = StepResultStatus.SUCCEEDED,
            output = msg.output,
        )
        msg.replyTo.tell(StepResult(msg.stepId, result))
        return this
    }

    private fun executeAsync(request: StepExecutionRequest): java.util.concurrent.CompletionStage<StepExecutionResult> {
        return when (request.stepKind) {
            StepKind.AGENT -> {
                val adapter = agentAdapters[request.backend]
                if (adapter == null) {
                    logger.warn("No adapter found for backend: ${request.backend}")
                    java.util.concurrent.CompletableFuture.completedFuture(
                        StepExecutionResult(
                            status = StepResultStatus.FAILED,
                            error = "No adapter found for backend: ${request.backend}",
                        )
                    )
                } else {
                    adapter.executeStep(request)
                }
            }
            StepKind.TOOL -> {
                val toolId = request.definitionRef
                val adapter = toolAdapters.values.firstOrNull()
                if (adapter == null) {
                    java.util.concurrent.CompletableFuture.completedFuture(
                        StepExecutionResult(
                            status = StepResultStatus.FAILED,
                            error = "No tool adapter available",
                        )
                    )
                } else {
                    val toolRequest = ToolInvocationRequest(
                        runId = request.runId,
                        stepId = request.stepId,
                        toolId = toolId,
                        input = request.input,
                        correlationId = request.correlationId,
                    )
                    adapter.invoke(toolRequest).thenApply { result ->
                        StepExecutionResult(
                            status = result.status,
                            output = result.output,
                            error = result.error,
                            metrics = result.metrics,
                        )
                    }
                }
            }
            StepKind.SKILL -> {
                val skillId = request.definitionRef
                val adapter = skillAdapters.values.firstOrNull()
                if (adapter == null) {
                    java.util.concurrent.CompletableFuture.completedFuture(
                        StepExecutionResult(
                            status = StepResultStatus.FAILED,
                            error = "No skill adapter available",
                        )
                    )
                } else {
                    val skillRequest = SkillInvocationRequest(
                        runId = request.runId,
                        stepId = request.stepId,
                        skillId = skillId,
                        input = request.input,
                        correlationId = request.correlationId,
                    )
                    adapter.invoke(skillRequest).thenApply { result ->
                        StepExecutionResult(
                            status = result.status,
                            output = result.output,
                            error = result.error,
                            metrics = result.metrics,
                        )
                    }
                }
            }
            else -> {
                java.util.concurrent.CompletableFuture.completedFuture(
                    StepExecutionResult(
                        status = StepResultStatus.SUCCEEDED,
                        output = request.input,
                    )
                )
            }
        }
    }
}

/**
 * Internal message used by the `pipeToSelf` pattern to deliver asynchronous step execution
 * results back into the [StepExecutor] actor's message loop.
 *
 * Upon construction, this message immediately forwards the [result] to the originating
 * [RunEntity] via the [replyTo] reference as a [StepResult] command. This ensures that
 * the `RunEntity` receives the step outcome regardless of whether the `StepExecutor`
 * processes this message further.
 *
 * @property stepId The identifier of the step whose execution has completed.
 * @property result The [StepExecutionResult] produced by the adapter, containing status, output,
 *                  error details, and execution metrics.
 * @property replyTo The [RunEntity] actor reference to receive the [StepResult] command.
 *
 * @see StepExecutor
 * @see StepResult
 */
internal data class StepResultInternal(
    val stepId: String,
    val result: StepExecutionResult,
    val replyTo: ActorRef<RunCommand>,
) : StepExecutorMessage {
    init {
        replyTo.tell(StepResult(stepId, result))
    }
}
