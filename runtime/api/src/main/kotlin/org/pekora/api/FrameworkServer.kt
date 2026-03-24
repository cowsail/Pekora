/**
 * Bootstrap and main entry point for the Pekora Agent Workflow Framework server.
 *
 * This file contains the [FrameworkServer] object which initializes the entire runtime:
 * actor system, cluster sharding, child actors, and the HTTP server. It serves as the
 * composition root that wires all framework components together.
 *
 * **Bootstrap sequence:**
 *
 * 1. Spawn the [WorkflowRegistry][org.pekora.registry.WorkflowRegistry] actor for
 *    template and version management.
 * 2. Spawn the [ApprovalManager][org.pekora.engine.ApprovalManager] actor for
 *    human-in-the-loop approval gates.
 * 3. Use [AdapterFactory] to read HOCON config and construct enabled [AgentRuntimeAdapter] instances.
 * 4. Create a [PolicyGuard][org.pekora.policy.PolicyGuard] instance and spawn the
 *    [StepExecutor][org.pekora.engine.StepExecutor] actor for executing workflow steps.
 * 5. Initialize [ClusterSharding] for [RunEntity][org.pekora.engine.RunEntity] so that
 *    run instances are distributed across cluster nodes and persisted via event sourcing.
 * 6. Build the HTTP route tree by composing [WorkflowRoutes], [RunRoutes], and [HealthRoutes].
 * 7. Bind the Pekko HTTP server to the configured host and port.
 *
 * @see FrameworkServer
 * @see WorkflowRoutes
 * @see RunRoutes
 * @see HealthRoutes
 */
package org.pekora.api

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity
import org.apache.pekko.http.javadsl.Http
import org.apache.pekko.http.javadsl.server.AllDirectives
import org.apache.pekko.persistence.typed.PersistenceId
import org.pekora.adapters.native.NativeAgentRegistry
import org.pekora.engine.*
import org.pekora.policy.PolicyGuard
import org.pekora.projection.RunProjectionStore
import org.pekora.registry.*
import org.slf4j.LoggerFactory

/**
 * Main server object that bootstraps the Pekora Agent Workflow Framework.
 *
 * [FrameworkServer] is a singleton that provides:
 *
 * - [create]: a factory method returning a Pekka [Behavior] that performs the full
 *   bootstrap sequence (actor spawning, cluster sharding initialization, HTTP binding).
 * - [main]: a JVM entry point that reads `HTTP_HOST` and `HTTP_PORT` from environment
 *   variables and starts the actor system.
 *
 * The bootstrap sequence initializes the following actors as children of the root
 * guardian:
 *
 * - **workflow-registry** -- [WorkflowRegistry][org.pekora.registry.WorkflowRegistry]
 *   for template/version storage.
 * - **approval-manager** -- [ApprovalManager][org.pekora.engine.ApprovalManager] for
 *   managing pending approval gates.
 * - **step-executor** -- [StepExecutor][org.pekora.engine.StepExecutor] for
 *   dispatching step execution to agent backends.
 *
 * Cluster sharding is configured for [RunEntity][org.pekora.engine.RunEntity] using
 * [RunEntityTypeKey], so each run is a sharded, event-sourced entity identified by its
 * run ID. The [PersistenceId] is derived from the entity type key and entity ID.
 *
 * @see WorkflowRoutes
 * @see RunRoutes
 * @see HealthRoutes
 * @see org.pekora.engine.RunEntity
 * @see org.pekora.registry.WorkflowRegistry
 */
object FrameworkServer {

    private val logger = LoggerFactory.getLogger(FrameworkServer::class.java)

    /**
     * Registry for native Pekko agent behaviors.
     *
     * Register agent behaviors here before the server starts (or at any time — actors
     * are spawned lazily on first dispatch):
     *
     * ```kotlin
     * FrameworkServer.nativeAgents.register("summarizer", PekoraAgentBehavior.create(::SummarizerAgent))
     * FrameworkServer.main(emptyArray())
     * ```
     *
     * Agents registered here are accessible in workflow YAML as `backend: native`.
     * The agent `id` in the YAML must match the name passed to [NativeAgentRegistry.register].
     */
    val nativeAgents: NativeAgentRegistry = NativeAgentRegistry()

    /**
     * Creates the root [Behavior] that bootstraps the framework.
     *
     * When the returned behavior is materialized by an [ActorSystem], it:
     *
     * 1. Spawns the workflow registry, approval manager, and step executor actors.
     * 2. Uses [AdapterFactory] to initialize only enabled adapters from HOCON config.
     * 3. Initializes cluster sharding for [RunEntity][org.pekora.engine.RunEntity].
     * 4. Builds the HTTP route tree from [WorkflowRoutes], [RunRoutes], and [HealthRoutes].
     * 5. Binds the Pekora HTTP server to the given [host] and [port].
     *
     * If the HTTP bind fails, the actor system is terminated.
     *
     * @param host The network interface to bind to. Defaults to `"0.0.0.0"` (all interfaces).
     * @param port The TCP port to listen on. Defaults to `8080`.
     * @return A [Behavior] of type `Void` (no external messages) for use as the root guardian.
     * @see main
     */
    fun create(
        host: String = "0.0.0.0",
        port: Int = 8080,
    ): Behavior<Void> = Behaviors.setup { ctx ->
        val system = ctx.system

        // Initialize workflow registry
        val registry = ctx.spawn(WorkflowRegistry.create(), "workflow-registry")
        logger.info("WorkflowRegistry started")

        // Initialize approval manager
        val approvalManager = ctx.spawn(ApprovalManager.create(), "approval-manager")
        logger.info("ApprovalManager started")

        // Initialize adapters from HOCON config (includes native adapter)
        val agentAdapters = AdapterFactory.createAdapters(system.settings().config(), system, nativeAgents)

        val distributedWorkers = DistributedWorkersSettings.fromConfig(system.settings().config())
        val workDispatch = WorkDispatchFactory.bootstrap(system, distributedWorkers)
        val runProjection = RunProjectionStore()

        // Initialize step executor with agent adapters, policy guard, and dispatch gateway
        val policyGuard = PolicyGuard()
        val stepExecutor = ctx.spawn(
            StepExecutor.create(
                agentAdapters = agentAdapters,
                policyGuard = policyGuard,
                stepDispatchGateway = workDispatch.stepDispatchGateway,
            ),
            "step-executor",
        )
        logger.info("StepExecutor started with adapters: ${agentAdapters.keys}")
        logger.info(
            "Distributed workers config: enabled={}, provider={}, embeddedWorkers={}x",
            distributedWorkers.enabled,
            distributedWorkers.provider,
            distributedWorkers.embeddedWorkers.replicas,
        )

        // Initialize cluster sharding for RunEntity
        val sharding = ClusterSharding.get(system)
        sharding.init(
            Entity.of(RunEntityTypeKey.typeKey) { entityContext ->
                RunEntity.create(
                    runId = entityContext.entityId,
                    persistenceId = PersistenceId.of(
                        entityContext.entityTypeKey.name(),
                        entityContext.entityId,
                    ),
                    stepExecutor = stepExecutor,
                    approvalManager = approvalManager,
                    registry = registry,
                    sharding = sharding,
                    eventObserver = runProjection::applyEvent,
                )
            }
        )
        logger.info("RunEntity cluster sharding initialized")

        WorkDispatchFactory.spawnEmbeddedWorkers(
            context = ctx,
            settings = distributedWorkers,
            workQueueProvider = workDispatch.workQueueProvider,
            agentAdapters = agentAdapters,
            sharding = sharding,
        )

        // Set up HTTP routes
        val allDirectives = object : AllDirectives() {}
        val workflowRoutes = WorkflowRoutes(registry, system)
        val runRoutes = RunRoutes(sharding, registry, approvalManager, runProjection, system)
        val healthRoutes = HealthRoutes(agentAdapters, system)

        val route = allDirectives.concat(
            workflowRoutes.routes(),
            runRoutes.routes(),
            healthRoutes.routes(),
        )

        // Start HTTP server
        Http.get(system).newServerAt(host, port).bind(route)
            .whenComplete { binding, error ->
                if (error != null) {
                    logger.error("Failed to bind HTTP server", error)
                    system.terminate()
                } else {
                    logger.info("HTTP server bound to ${binding.localAddress()}")
                }
            }

        Behaviors.empty()
    }

    /**
     * JVM entry point for the framework server.
     *
     * Reads configuration from environment variables:
     * - `HTTP_HOST` -- the bind address (default: `"0.0.0.0"`).
     * - `HTTP_PORT` -- the listen port (default: `8080`).
     *
     * Creates an [ActorSystem] named `"AgentFramework"` with the root behavior produced
     * by [create].
     *
     * @param args Command-line arguments (currently unused).
     * @see create
     */
    @JvmStatic
    fun main(args: Array<String>) {
        val host = System.getenv("HTTP_HOST") ?: "0.0.0.0"
        val port = System.getenv("HTTP_PORT")?.toIntOrNull() ?: 8080
        ActorSystem.create(create(host, port), "AgentFramework")
    }
}
