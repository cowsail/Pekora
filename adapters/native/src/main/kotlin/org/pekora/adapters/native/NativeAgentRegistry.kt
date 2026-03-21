/**
 * Registry for native Pekko agents in the Pekora framework.
 *
 * Register named agent behaviors before or after the framework server starts.
 * Actors are spawned lazily on first dispatch and cached as singletons so
 * stateful agents retain their state across multiple step calls.
 *
 * ## Usage
 *
 * ```kotlin
 * // At application startup:
 * server.nativeAgents.register("summarizer", PekoraAgentBehavior.create(::SummarizerAgent))
 * server.nativeAgents.register("classifier", AsyncPekoraAgentBehavior.create(::ClassifierAgent))
 *
 * // In workflow YAML:
 * // agents:
 * //   - id: summarizer
 * //     backend: native
 * ```
 */
package org.pekora.adapters.native

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.Props
import org.pekora.adapters.generic.GenericActorRequest
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Thread-safe registry that maps agent names to their Pekko actor behaviors.
 *
 * Call [register] to add an agent before the first workflow step that references it
 * is dispatched. On first dispatch, [NativeAdapter] calls [getOrSpawn] to lazily
 * spawn and cache the actor under the actor system's guardian.
 */
class NativeAgentRegistry {

    private val logger = LoggerFactory.getLogger(NativeAgentRegistry::class.java)

    // Unique suffix per registry instance prevents actor name collisions when multiple
    // registries share the same ActorSystem (e.g., in tests).
    private val instanceId = UUID.randomUUID().toString().take(8)

    private val behaviors = ConcurrentHashMap<String, Behavior<GenericActorRequest>>()
    private val actors = ConcurrentHashMap<String, ActorRef<GenericActorRequest>>()

    /**
     * Registers an agent behavior under [name].
     *
     * If [name] is already registered, the previous registration is replaced.
     * If the actor was already spawned for the old behavior, the cached ref is evicted —
     * the next dispatch will spawn a fresh actor from the new behavior.
     *
     * @param name The agent name referenced by the `id` field in the workflow YAML `agents:` block.
     * @param behavior The Pekka behavior to spawn. Use [PekoraAgentBehavior.create] or
     *   [AsyncPekoraAgentBehavior.create] to build behaviors from your subclasses.
     */
    fun register(name: String, behavior: Behavior<GenericActorRequest>) {
        behaviors[name] = behavior
        actors.remove(name) // evict stale ref if behavior changed
        logger.info("Registered native agent '{}'", name)
    }

    /**
     * Returns the set of registered agent names.
     */
    fun registeredNames(): Set<String> = behaviors.keys.toSet()

    /**
     * Returns the cached [ActorRef] for [name], or spawns one from the registered behavior.
     *
     * Returns `null` if no behavior has been registered for [name].
     *
     * This method is internal — it is called by [NativeAdapter] during step dispatch.
     *
     * @param name The agent name to look up.
     * @param system The actor system used to spawn the actor if not yet cached.
     */
    internal fun getOrSpawn(
        name: String,
        system: ActorSystem<*>,
    ): ActorRef<GenericActorRequest>? {
        actors[name]?.let { return it }

        val behavior = behaviors[name] ?: run {
            logger.warn("No native agent registered for name '{}'", name)
            return null
        }

        return actors.computeIfAbsent(name) { key ->
            val actorName = "native-agent-$key-$instanceId"
            val ref = system.systemActorOf(behavior, actorName, Props.empty())
            logger.info("Spawned native agent actor '{}' at path {}", key, ref.path())
            ref
        }
    }
}
