/**
 * Lifecycle scope for native Pekko agents registered with [NativeAgentRegistry].
 *
 * The scope controls how many actor instances are created and how long they live.
 */
package org.pekora.adapters.native

/**
 * Lifecycle scope for a native agent registration.
 *
 * @see NativeAgentRegistry.register
 */
enum class AgentScope {

    /**
     * One actor per registered name, shared across all runs.
     *
     * State accumulates indefinitely — suitable for stateless agents, shared model clients,
     * or caches that intentionally span runs. This is the default.
     */
    SINGLETON,

    /**
     * One actor per `(name, runId)` pair.
     *
     * A fresh actor is spawned on the first step dispatch for a given run, and stopped
     * automatically when [NativeAgentRegistry.cleanupRun] is called (triggered by
     * [org.pekora.engine.RunEntity] on run completion, failure, or cancellation).
     *
     * Use this for agents that accumulate per-run state such as conversation history or
     * run-scoped context, where state should not leak between runs.
     */
    PER_RUN,
}
