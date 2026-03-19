/**
 * # StepExecutor — Broker for Step Execution Dispatch
 *
 * This file defines [StepExecutor], the actor that dispatches step execution requests
 * from [RunEntity] to the appropriate [AgentRuntimeAdapter] based on the step's backend.
 *
 * ## Broker Pattern
 *
 * Rather than having [RunEntity] interact directly with external runtimes, all step execution
 * is funneled through this broker actor. This provides a single point for:
 * - Adapter lookup and dispatch based on backend identifier.
 * - Policy enforcement via [PolicyGuard][org.pekora.policy.PolicyGuard].
 * - Centralized error handling and logging.
 *
 * ## Async Handling with pipeToSelf
 *
 * Step adapters return [CompletionStage][java.util.concurrent.CompletionStage] results to
 * support non-blocking I/O (e.g., HTTP calls to external LLM APIs). The `StepExecutor` uses
 * Pekko's `pipeToSelf` pattern to bridge the async `CompletionStage` back into the actor's
 * message-processing loop.
 *
 * ## Message Protocol
 *
 * - [ExecuteStep] — Requests execution of a workflow step via the appropriate adapter.
 * - [ExecuteResultStep] — Handles terminal RESULT steps that simply pass output through.
 * - [StepResultInternal] — Internal message used by `pipeToSelf` to forward async results.
 *
 * @see RunEntity
 * @see org.pekora.adapters.AgentRuntimeAdapter
 */
package org.pekora.engine

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import org.pekora.adapters.AgentRuntimeAdapter
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
 * @property request The fully resolved [StepExecutionRequest] containing step kind, backend,
 *                   input parameters, and execution constraints.
 * @property replyTo The [RunEntity] actor reference to receive the [StepResult] upon completion.
 */
data class ExecuteStep(
    val request: StepExecutionRequest,
    val replyTo: ActorRef<RunCommand>,
) : StepExecutorMessage

/**
 * Message requesting execution of a terminal RESULT step.
 *
 * RESULT steps do not invoke any external adapter; they pass the resolved output
 * map back to the [RunEntity] as a successful [StepResult].
 *
 * @property runId The workflow run identifier.
 * @property stepId The identifier of the RESULT step.
 * @property output The resolved output map to return as the step's result.
 * @property replyTo The [RunEntity] actor reference to receive the [StepResult].
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
 * @param context The typed actor context.
 * @param agentAdapters Map of backend identifiers to [AgentRuntimeAdapter] instances.
 * @param policyGuard The [PolicyGuard] used to enforce execution policies before dispatching steps.
 */
class StepExecutor(
    context: ActorContext<StepExecutorMessage>,
    private val agentAdapters: Map<String, AgentRuntimeAdapter>,
    private val policyGuard: PolicyGuard,
) : AbstractBehavior<StepExecutorMessage>(context) {

    companion object {
        private val logger = LoggerFactory.getLogger(StepExecutor::class.java)

        /**
         * Factory method that creates a new [StepExecutor] behavior.
         *
         * @param agentAdapters Map of backend identifiers to [AgentRuntimeAdapter] instances. Defaults to empty.
         * @param policyGuard The [PolicyGuard] for policy enforcement. Defaults to a new [PolicyGuard] instance.
         * @return A [Behavior] that, when materialized, produces a fully initialized [StepExecutor].
         */
        fun create(
            agentAdapters: Map<String, AgentRuntimeAdapter> = emptyMap(),
            policyGuard: PolicyGuard = PolicyGuard(),
        ): Behavior<StepExecutorMessage> = Behaviors.setup { ctx ->
            StepExecutor(ctx, agentAdapters, policyGuard)
        }
    }

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
        val adapter = agentAdapters[request.backend]
        return if (adapter == null) {
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
}

/**
 * Internal message used by the `pipeToSelf` pattern to deliver asynchronous step execution
 * results back into the [StepExecutor] actor's message loop.
 *
 * Upon construction, this message immediately forwards the [result] to the originating
 * [RunEntity] via the [replyTo] reference as a [StepResult] command.
 *
 * @property stepId The identifier of the step whose execution has completed.
 * @property result The [StepExecutionResult] produced by the adapter.
 * @property replyTo The [RunEntity] actor reference to receive the [StepResult] command.
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
