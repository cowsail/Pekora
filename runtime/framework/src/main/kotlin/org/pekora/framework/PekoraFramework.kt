package org.pekora.framework

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.cluster.typed.Join
import org.apache.pekko.persistence.typed.PersistenceId
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.native.NativeAgentRegistry
import org.pekora.engine.ApprovalManager
import org.pekora.engine.RunEntity
import org.pekora.engine.RunEntityTypeKey
import org.pekora.engine.StepExecutor
import org.pekora.policy.PolicyGuard
import org.pekora.projection.DefaultRunEventProjector
import org.pekora.projection.InMemoryRunNotificationStore
import org.pekora.projection.InMemoryRunProjectionStore
import org.pekora.projection.RunNotificationStore
import org.pekora.projection.RunProjectionStore
import org.pekora.registry.WorkflowRegistry
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

data class PekoraFrameworkOptions(
    val nativeAgents: NativeAgentRegistry = NativeAgentRegistry(),
    val adapters: Map<String, AgentRuntimeAdapter> = emptyMap(),
    val plugins: List<PekoraPlugin> = emptyList(),
    val runProjectionStoreFactory: () -> RunProjectionStore = { InMemoryRunProjectionStore() },
    val runNotificationStoreFactory: () -> RunNotificationStore = { InMemoryRunNotificationStore() },
)

class PekoraFrameworkHandle internal constructor(
    val system: ActorSystem<Void>,
    private val runtimeFuture: CompletableFuture<PekoraFrameworkRuntime>,
) {
    fun runtime(): CompletionStage<PekoraFrameworkRuntime> = runtimeFuture

    fun awaitRuntime(): PekoraFrameworkRuntime = runtimeFuture.get()

    fun client(): PekoraFrameworkClient = PekoraFrameworkClient(awaitRuntime())
}

object PekoraFramework {
    private val logger = LoggerFactory.getLogger(PekoraFramework::class.java)

    fun create(options: PekoraFrameworkOptions = PekoraFrameworkOptions()): Behavior<Void> {
        val runtimeFuture = CompletableFuture<PekoraFrameworkRuntime>()
        return create(options, runtimeFuture)
    }

    fun start(
        systemName: String = "PekoraFramework",
        config: Config = ConfigFactory.load(),
        options: PekoraFrameworkOptions = PekoraFrameworkOptions(),
    ): PekoraFrameworkHandle {
        val runtimeFuture = CompletableFuture<PekoraFrameworkRuntime>()
        val system = ActorSystem.create(create(options, runtimeFuture), systemName, config)
        return PekoraFrameworkHandle(system, runtimeFuture)
    }

    private fun create(
        options: PekoraFrameworkOptions,
        runtimeFuture: CompletableFuture<PekoraFrameworkRuntime>,
    ): Behavior<Void> = Behaviors.setup { ctx ->
        val system = ctx.system
        val cluster = Cluster.get(system)
        cluster.manager().tell(Join.create(cluster.selfMember().address()))

        val registry = ctx.spawn(WorkflowRegistry.create(), "workflow-registry")
        logger.info("WorkflowRegistry started")

        val approvalManager = ctx.spawn(ApprovalManager.create(), "approval-manager")
        logger.info("ApprovalManager started")

        val agentAdapters = AdapterFactory.createAdapters(system.settings().config(), system, options.nativeAgents) + options.adapters
        val distributedWorkers = DistributedWorkersSettings.fromConfig(system.settings().config())
        val workDispatch = WorkDispatchFactory.bootstrap(system, distributedWorkers)
        val runProjection = options.runProjectionStoreFactory()
        val runNotifications = options.runNotificationStoreFactory()
        val runProjector = DefaultRunEventProjector(runProjection, runNotifications)

        val stepExecutor = ctx.spawn(
            StepExecutor.create(
                agentAdapters = agentAdapters,
                policyGuard = PolicyGuard(),
                stepDispatchGateway = workDispatch.stepDispatchGateway,
            ),
            "step-executor",
        )
        logger.info("StepExecutor started with adapters: {}", agentAdapters.keys)

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
                    eventObserver = runProjector::project,
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

        val runtime = PekoraFrameworkRuntime(
            system = system,
            registry = registry,
            approvalManager = approvalManager,
            sharding = sharding,
            stepExecutor = stepExecutor,
            runProjection = runProjection,
            runNotifications = runNotifications,
            agentAdapters = agentAdapters,
            distributedWorkers = distributedWorkers,
        )

        runtimeFuture.complete(runtime)
        options.plugins.forEach { plugin -> plugin.install(runtime) }

        Behaviors.empty()
    }
}
