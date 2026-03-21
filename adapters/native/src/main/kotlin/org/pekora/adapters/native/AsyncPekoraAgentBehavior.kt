/**
 * SDK base class for asynchronous native Pekko agents.
 *
 * Use this when your agent logic involves non-blocking async work such as HTTP calls,
 * database queries, or calls to the Claude API. The [CompletionStage] returned by
 * [handleStep] completes on whatever thread the future resolves on; the result is
 * forwarded directly to the reply target via a thread-safe [ActorRef.tell] call.
 *
 * ## Usage
 *
 * ```kotlin
 * class LlmAgent(context: ActorContext<GenericActorRequest>) : AsyncPekoraAgentBehavior(context) {
 *     override fun handleStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
 *         return myHttpClient.callLlm(request.input)
 *             .thenApply { response ->
 *                 StepExecutionResult(StepResultStatus.SUCCEEDED, output = mapOf("result" to response))
 *             }
 *     }
 * }
 *
 * server.nativeAgents.register("llm", AsyncPekoraAgentBehavior.create(::LlmAgent))
 * ```
 *
 * @see PekoraAgentBehavior for synchronous agents
 */
package org.pekora.adapters.native

import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import org.pekora.adapters.generic.GenericActorRequest
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletionStage

/**
 * Abstract base behavior for asynchronous native Pekko agents.
 *
 * On each [GenericActorRequest], [handleStep] is called on the actor thread and returns a
 * [CompletionStage]. When the future completes (on any thread), the result is sent to the
 * original [replyTo] reference via a thread-safe tell. The actor itself is never blocked.
 *
 * If [handleStep] throws synchronously or the returned future fails, a FAILED result is
 * delivered and the actor continues running normally.
 *
 * **Note:** Because the reply is sent from a completion callback rather than the actor thread,
 * state mutations should only be made within [handleStep] before returning the future, not
 * inside the future's callbacks.
 *
 * @param context The Pekko actor context provided by [Behaviors.setup].
 */
abstract class AsyncPekoraAgentBehavior(
    context: ActorContext<GenericActorRequest>,
) : AbstractBehavior<GenericActorRequest>(context) {

    companion object {
        private val logger = LoggerFactory.getLogger(AsyncPekoraAgentBehavior::class.java)

        /**
         * Creates a [Behavior] factory for use with [NativeAgentRegistry.register].
         *
         * @param factory A constructor reference or lambda that creates your behavior instance.
         */
        fun <T : AsyncPekoraAgentBehavior> create(
            factory: (ActorContext<GenericActorRequest>) -> T,
        ): Behavior<GenericActorRequest> = Behaviors.setup { ctx -> factory(ctx) }
    }

    override fun createReceive(): Receive<GenericActorRequest> =
        newReceiveBuilder()
            .onMessage(GenericActorRequest::class.java) { msg ->
                val replyTo = msg.replyTo
                val future: CompletionStage<StepExecutionResult> = try {
                    handleStep(msg.request)
                } catch (e: Exception) {
                    logger.error(
                        "Native async agent '{}' threw during handleStep for step {}",
                        javaClass.simpleName, msg.request.stepId, e,
                    )
                    replyTo.tell(
                        StepExecutionResult(
                            status = StepResultStatus.FAILED,
                            error = e.message ?: "Agent threw an exception",
                        )
                    )
                    return@onMessage Behaviors.same()
                }

                // ActorRef.tell() is thread-safe — safe to call from the future's callback thread.
                future.whenComplete { result, ex ->
                    if (ex != null) {
                        logger.error(
                            "Native async agent '{}' future failed for step {}",
                            javaClass.simpleName, msg.request.stepId, ex,
                        )
                        replyTo.tell(
                            StepExecutionResult(
                                status = StepResultStatus.FAILED,
                                error = ex.message ?: "Future failed",
                            )
                        )
                    } else {
                        replyTo.tell(result)
                    }
                }
                Behaviors.same()
            }
            .build()

    /**
     * Executes a workflow step asynchronously.
     *
     * Return a [CompletionStage] that completes with the step result. The method itself
     * runs on the actor thread, so only use it to *start* the async work and return the
     * future — do not block here.
     *
     * @param request The step execution request with resolved inputs and constraints.
     * @return A future that resolves to the normalized step result.
     */
    abstract fun handleStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult>
}
