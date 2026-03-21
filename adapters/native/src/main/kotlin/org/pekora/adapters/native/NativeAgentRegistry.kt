/**
 * Registry for native Pekko agents in the Pekora framework.
 *
 * Supports three agent scopes:
 *
 * - **[AgentScope.SINGLETON]** (default) — one actor per registered name, shared across all
 *   runs. Actors are spawned lazily on first dispatch and cached indefinitely. Suitable for
 *   stateless agents or caches.
 *
 * - **[AgentScope.PER_RUN]** — one actor per `(name, runId)` pair, spawned lazily and
 *   stopped when [cleanupRun] is called. Use this for agents that accumulate per-run state
 *   such as conversation history.
 *
 * - **PER_INVOCATION** (registered via [registerPerInvocation] or [registerPerInvocationAsync])
 *   — a plain function called fresh for every step execution. No actor is created or cached.
 *   Because each invocation runs independently, long-running or blocking handlers never block
 *   other runs. Use this for approval gates, external callbacks, and one-shot side effects.
 *
 * ## Usage
 *
 * ```kotlin
 * // SINGLETON (default)
 * registry.register("classifier", PekoraAgentBehavior.create(::Classifier))
 *
 * // PER_RUN — fresh state per run
 * registry.register("chat", PekoraAgentBehavior.create(::ChatAgent), AgentScope.PER_RUN)
 *
 * // PER_INVOCATION — sync lambda
 * registry.registerPerInvocation("echo") { request ->
 *     StepExecutionResult(StepResultStatus.SUCCEEDED, output = request.input)
 * }
 *
 * // PER_INVOCATION — async lambda (non-blocking)
 * registry.registerPerInvocationAsync("approval") { request ->
 *     approvalService.request(request.runId)
 *         .thenApply { approved -> StepExecutionResult(StepResultStatus.SUCCEEDED) }
 * }
 * ```
 */
package org.pekora.adapters.native

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.Props
import org.apache.pekko.actor.typed.javadsl.Adapter
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.pekora.adapters.generic.GenericActorRequest
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap

/**
 * Thread-safe registry that maps agent names to behaviors or invocation handlers.
 *
 * Call [register], [registerPerInvocation], or [registerPerInvocationAsync] to add agents.
 * Call [dispatch] (used internally by [NativeAdapter]) to execute a step.
 * Call [cleanupRun] when a run terminates to release PER_RUN actors.
 */
class NativeAgentRegistry {

    private val logger = LoggerFactory.getLogger(NativeAgentRegistry::class.java)

    // Unique suffix per registry instance prevents actor name collisions when multiple
    // registries share the same ActorSystem (common in tests).
    private val instanceId = UUID.randomUUID().toString().take(8)

    // SINGLETON and PER_RUN behaviors — value is (behavior, scope)
    private val behaviors = ConcurrentHashMap<String, Pair<Behavior<GenericActorRequest>, AgentScope>>()

    // Cached singleton actor refs — keyed by agent name
    private val singletonActors = ConcurrentHashMap<String, ActorRef<GenericActorRequest>>()

    // Cached per-run actor refs — keyed by "$name::$runId"
    private val perRunActors = ConcurrentHashMap<String, ActorRef<GenericActorRequest>>()

    // PER_INVOCATION handlers — each call gets a fresh invocation, no actor created
    private val perInvocationHandlers =
        ConcurrentHashMap<String, (StepExecutionRequest) -> CompletionStage<StepExecutionResult>>()

    // ── Registration ──────────────────────────────────────────────────────────

    /**
     * Registers an actor-backed agent with the given [scope].
     *
     * If [name] is already registered, the previous registration is replaced and any cached
     * actor ref for that name is evicted (a fresh actor is spawned on next dispatch).
     *
     * @param name The agent name — must match the `id` field in the workflow YAML `agents:` block.
     * @param behavior The Pekko behavior. Use [PekoraAgentBehavior.create] or
     *   [AsyncPekoraAgentBehavior.create] to build behaviors from your subclasses.
     * @param scope [AgentScope.SINGLETON] (default) or [AgentScope.PER_RUN].
     */
    fun register(
        name: String,
        behavior: Behavior<GenericActorRequest>,
        scope: AgentScope = AgentScope.SINGLETON,
    ) {
        behaviors[name] = behavior to scope
        singletonActors.remove(name)
        // perRunActors are keyed by "$name::$runId" — remove all for this name
        perRunActors.keys.filter { it.startsWith("$name::") }.forEach { perRunActors.remove(it) }
        logger.info("Registered native agent '{}' (scope={})", name, scope)
    }

    /**
     * Registers a synchronous per-invocation handler.
     *
     * The [handler] is called fresh for every step execution. No actor is created.
     * If [handler] blocks, it runs on a JVM thread pool — the actor system is not blocked
     * and other runs dispatching to the same agent name are unaffected.
     *
     * @param name The agent name — must match the `id` in workflow YAML `agents:`.
     * @param handler A function that receives a [StepExecutionRequest] and returns a result.
     */
    fun registerPerInvocation(
        name: String,
        handler: (StepExecutionRequest) -> StepExecutionResult,
    ) {
        perInvocationHandlers[name] = { request ->
            CompletableFuture.supplyAsync { handler(request) }
        }
        logger.info("Registered native agent '{}' (scope=PER_INVOCATION, sync)", name)
    }

    /**
     * Registers an asynchronous per-invocation handler.
     *
     * The [handler] is called fresh for every step execution and returns a [CompletionStage].
     * No actor is created — execution is entirely non-blocking from the framework's perspective.
     *
     * @param name The agent name — must match the `id` in workflow YAML `agents:`.
     * @param handler A function that returns a [CompletionStage] resolving to the step result.
     */
    fun registerPerInvocationAsync(
        name: String,
        handler: (StepExecutionRequest) -> CompletionStage<StepExecutionResult>,
    ) {
        perInvocationHandlers[name] = handler
        logger.info("Registered native agent '{}' (scope=PER_INVOCATION, async)", name)
    }

    /**
     * Returns the set of all registered agent names across all scopes.
     */
    fun registeredNames(): Set<String> =
        behaviors.keys.toSet() + perInvocationHandlers.keys.toSet()

    // ── Dispatch ──────────────────────────────────────────────────────────────

    /**
     * Dispatches a step execution to the registered agent, routing by scope.
     *
     * Called internally by [NativeAdapter]. Not intended for direct use.
     *
     * @param name The agent name (from [StepExecutionRequest.definitionRef]).
     * @param runId The run identifier (from [StepExecutionRequest.runId]).
     * @param request The full step execution request.
     * @param system The actor system used for spawning and scheduling.
     * @param askTimeout Timeout for actor ask patterns.
     */
    internal fun dispatch(
        name: String,
        runId: String,
        request: StepExecutionRequest,
        system: ActorSystem<*>,
        askTimeout: Duration,
    ): CompletionStage<StepExecutionResult> {
        // PER_INVOCATION takes priority — check before actor-based paths
        perInvocationHandlers[name]?.let { handler ->
            logger.info("NativeRegistry dispatching '{}' (PER_INVOCATION) for run {}", name, runId)
            return try {
                handler(request)
            } catch (e: Exception) {
                logger.error("PER_INVOCATION handler '{}' threw for step {}", name, request.stepId, e)
                CompletableFuture.completedFuture(
                    StepExecutionResult(StepResultStatus.FAILED, error = e.message ?: "Handler threw")
                )
            }
        }

        val (behavior, scope) = behaviors[name] ?: run {
            logger.error("No native agent registered for name '{}' (step '{}')", name, request.stepId)
            return CompletableFuture.completedFuture(
                StepExecutionResult(StepResultStatus.FAILED, error = "No native agent registered for name '$name'")
            )
        }

        val actorRef = when (scope) {
            AgentScope.SINGLETON -> getOrSpawnSingleton(name, behavior, system)
            AgentScope.PER_RUN -> getOrSpawnPerRun(name, runId, behavior, system)
        }

        logger.info("NativeRegistry dispatching '{}' (scope={}) for run {}", name, scope, runId)
        return AskPattern.ask(
            actorRef,
            { replyTo -> GenericActorRequest(request, replyTo) },
            askTimeout,
            system.scheduler(),
        ).exceptionally { ex ->
            logger.error("Native agent '{}' ask failed for step '{}'", name, request.stepId, ex)
            StepExecutionResult(StepResultStatus.FAILED, error = "Native agent ask failed: ${ex.message}")
        }
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────

    /**
     * Stops and removes all PER_RUN actors associated with [runId].
     *
     * Called by [NativeAdapter] when [org.pekora.engine.RunEntity] signals run termination.
     * No-op for SINGLETON or PER_INVOCATION agents.
     *
     * @param runId The run whose actors should be released.
     * @param system The actor system used to stop the actors.
     */
    internal fun cleanupRun(runId: String, system: ActorSystem<*>) {
        val prefix = "::$runId"
        val keys = perRunActors.keys.filter { it.endsWith(prefix) }
        if (keys.isEmpty()) return

        val classicSystem = Adapter.toClassic(system)
        for (key in keys) {
            val ref = perRunActors.remove(key) ?: continue
            try {
                classicSystem.stop(Adapter.toClassic(ref))
                logger.info("Stopped PER_RUN actor for key '{}' (run {})", key, runId)
            } catch (e: Exception) {
                logger.warn("Failed to stop PER_RUN actor for key '{}': {}", key, e.message)
            }
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private fun getOrSpawnSingleton(
        name: String,
        behavior: Behavior<GenericActorRequest>,
        system: ActorSystem<*>,
    ): ActorRef<GenericActorRequest> =
        singletonActors.computeIfAbsent(name) { key ->
            val actorName = "native-agent-$key-$instanceId"
            val ref = system.systemActorOf(behavior, actorName, Props.empty())
            logger.info("Spawned SINGLETON native agent '{}' at path {}", key, ref.path())
            ref
        }

    private fun getOrSpawnPerRun(
        name: String,
        runId: String,
        behavior: Behavior<GenericActorRequest>,
        system: ActorSystem<*>,
    ): ActorRef<GenericActorRequest> {
        val key = "$name::$runId"
        return perRunActors.computeIfAbsent(key) {
            val actorName = "native-agent-$name-$runId-$instanceId"
            val ref = system.systemActorOf(behavior, actorName, Props.empty())
            logger.info("Spawned PER_RUN native agent '{}' for run {} at path {}", name, runId, ref.path())
            ref
        }
    }
}
