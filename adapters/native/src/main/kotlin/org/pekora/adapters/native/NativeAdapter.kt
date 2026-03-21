/**
 * Adapter that dispatches workflow steps to native Pekko actors or per-invocation handlers.
 *
 * Registered under backend identifier `"native"`. Steps that declare `backend: native`
 * in their agent definition are routed here. The adapter looks up the agent by
 * [StepExecutionRequest.definitionRef] in the [NativeAgentRegistry] and dispatches
 * according to the registered scope:
 *
 * - **PER_INVOCATION** — calls the registered lambda directly, no actor overhead.
 * - **SINGLETON** — asks the cached singleton actor.
 * - **PER_RUN** — asks a per-run actor (spawned on first dispatch for this run).
 *
 * Call [cleanupRun] when a run ends to release PER_RUN actors (this is done automatically
 * by [org.pekora.engine.StepExecutor] when [org.pekora.engine.RunEntity] signals termination).
 *
 * @see NativeAgentRegistry
 * @see PekoraAgentBehavior
 * @see AsyncPekoraAgentBehavior
 */
package org.pekora.adapters.native

import org.apache.pekko.actor.typed.ActorSystem
import org.pekora.adapters.AdapterHealth
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.CleanableAdapter
import org.pekora.adapters.HealthStatus
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * [AgentRuntimeAdapter] implementation for in-process Pekko actor agents and per-invocation handlers.
 *
 * @property registry The registry of named native agent behaviors and handlers.
 * @property system The actor system used for spawning agents and the ask-pattern scheduler.
 * @property askTimeout How long to wait for an agent actor to reply before timing out.
 */
class NativeAdapter(
    private val registry: NativeAgentRegistry,
    private val system: ActorSystem<*>,
    private val askTimeout: Duration = Duration.ofSeconds(300),
) : AgentRuntimeAdapter, CleanableAdapter {

    private val logger = LoggerFactory.getLogger(NativeAdapter::class.java)

    override val backendId: String = "native"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        val agentName = request.definitionRef.ifEmpty { request.stepId }
        logger.info("NativeAdapter dispatching step '{}' to agent '{}'", request.stepId, agentName)
        return registry.dispatch(agentName, request.runId, request, system, askTimeout)
    }

    /**
     * Stops and removes all PER_RUN actors associated with [runId].
     *
     * Called by [org.pekora.engine.StepExecutor] when a run terminates.
     */
    override fun cleanupRun(runId: String) {
        logger.debug("NativeAdapter cleaning up PER_RUN actors for run '{}'", runId)
        registry.cleanupRun(runId, system)
    }

    /**
     * Returns HEALTHY when agents are registered, UNKNOWN when the registry is empty.
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
