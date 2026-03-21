/**
 * Adapter that dispatches workflow steps to native Pekko actors.
 *
 * Registered under backend identifier `"native"`. Steps that declare `backend: native`
 * in their agent definition are routed here. The adapter looks up the agent by
 * [StepExecutionRequest.definitionRef] (which is the agent `id` from the workflow YAML)
 * in the [NativeAgentRegistry], spawns the actor on first dispatch, and sends the step
 * request via the Pekko ask pattern.
 *
 * @see NativeAgentRegistry
 * @see PekoraAgentBehavior
 * @see AsyncPekoraAgentBehavior
 */
package org.pekora.adapters.native

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.pekora.adapters.AdapterHealth
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.HealthStatus
import org.pekora.adapters.generic.GenericActorRequest
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * [AgentRuntimeAdapter] implementation for in-process Pekko actor agents.
 *
 * @property registry The registry of named native agent behaviors.
 * @property system The actor system used for spawning agents and the ask-pattern scheduler.
 * @property askTimeout How long to wait for an agent actor to reply before timing out.
 */
class NativeAdapter(
    private val registry: NativeAgentRegistry,
    private val system: ActorSystem<*>,
    private val askTimeout: Duration = Duration.ofSeconds(300),
) : AgentRuntimeAdapter {

    private val logger = LoggerFactory.getLogger(NativeAdapter::class.java)

    override val backendId: String = "native"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        val agentName = request.definitionRef.ifEmpty { request.stepId }
        logger.info("NativeAdapter dispatching step '{}' to agent '{}'", request.stepId, agentName)

        val actorRef = registry.getOrSpawn(agentName, system) ?: run {
            logger.error("No native agent registered for name '{}' (step '{}')", agentName, request.stepId)
            return CompletableFuture.completedFuture(
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "No native agent registered for name '$agentName'",
                )
            )
        }

        return AskPattern.ask(
            actorRef,
            { replyTo -> GenericActorRequest(request, replyTo) },
            askTimeout,
            system.scheduler(),
        ).exceptionally { ex ->
            logger.error("Native agent '{}' ask timed out or failed for step '{}'", agentName, request.stepId, ex)
            StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "Native agent ask failed: ${ex.message}",
            )
        }
    }

    /**
     * Native agents are always in-process. Health is reported as HEALTHY unless the
     * registry has no agents registered (in which case it's UNKNOWN).
     */
    override fun healthCheck(): CompletionStage<AdapterHealth> {
        val names = registry.registeredNames()
        return CompletableFuture.completedFuture(
            if (names.isEmpty()) {
                AdapterHealth(backendId, HealthStatus.UNKNOWN, "No native agents registered")
            } else {
                AdapterHealth(backendId, HealthStatus.HEALTHY, "Registered agents: ${names.joinToString()}")
            }
        )
    }
}
