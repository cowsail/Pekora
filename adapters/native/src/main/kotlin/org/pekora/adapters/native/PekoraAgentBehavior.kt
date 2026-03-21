/**
 * SDK base class for synchronous native Pekko agents.
 *
 * Extend this class to implement an agent that runs inside the Pekko actor system.
 * Override [handleStep] with your agent logic — the framework handles all message
 * routing, reply protocol, and error recovery.
 *
 * ## Usage
 *
 * ```kotlin
 * class EchoAgent(context: ActorContext<GenericActorRequest>) : PekoraAgentBehavior(context) {
 *     override fun handleStep(request: StepExecutionRequest): StepExecutionResult =
 *         StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = request.input)
 * }
 *
 * // Register with the framework
 * server.nativeAgents.register("echo", PekoraAgentBehavior.create(::EchoAgent))
 * ```
 *
 * Native agents declared in workflow YAML:
 * ```yaml
 * agents:
 *   - id: echo
 *     backend: native
 * ```
 *
 * @see AsyncPekoraAgentBehavior for agents that perform non-blocking async work
 * @see NativeAgentRegistry for how agents are registered and discovered
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

/**
 * Abstract base behavior for synchronous native Pekko agents.
 *
 * Handles the [GenericActorRequest] protocol: receives a request, calls [handleStep],
 * and replies to the sender with a [StepExecutionResult]. The actor remains alive
 * after each step (singleton per registration), so it can maintain state across
 * multiple dispatches within a run.
 *
 * If [handleStep] throws, the exception is caught and a FAILED result is returned —
 * the actor does not crash and remains available for subsequent steps.
 *
 * @param context The Pekko actor context provided by [Behaviors.setup].
 */
abstract class PekoraAgentBehavior(
    context: ActorContext<GenericActorRequest>,
) : AbstractBehavior<GenericActorRequest>(context) {

    companion object {
        private val logger = LoggerFactory.getLogger(PekoraAgentBehavior::class.java)

        /**
         * Creates a [Behavior] factory for use with [NativeAgentRegistry.register].
         *
         * @param factory A constructor reference or lambda that creates your behavior instance.
         */
        fun <T : PekoraAgentBehavior> create(
            factory: (ActorContext<GenericActorRequest>) -> T,
        ): Behavior<GenericActorRequest> = Behaviors.setup { ctx -> factory(ctx) }
    }

    override fun createReceive(): Receive<GenericActorRequest> =
        newReceiveBuilder()
            .onMessage(GenericActorRequest::class.java) { msg ->
                val result = try {
                    handleStep(msg.request)
                } catch (e: Exception) {
                    logger.error("Native agent '{}' threw during handleStep for step {}", javaClass.simpleName, msg.request.stepId, e)
                    StepExecutionResult(status = StepResultStatus.FAILED, error = e.message ?: "Agent threw an exception")
                }
                msg.replyTo.tell(result)
                Behaviors.same()
            }
            .build()

    /**
     * Executes a workflow step synchronously.
     *
     * This method is called on the actor's thread. Do not perform blocking I/O here —
     * use [AsyncPekoraAgentBehavior] instead if you need non-blocking async work.
     *
     * @param request The step execution request with resolved inputs and constraints.
     * @return The normalized step result to return to the workflow.
     */
    abstract fun handleStep(request: StepExecutionRequest): StepExecutionResult
}
