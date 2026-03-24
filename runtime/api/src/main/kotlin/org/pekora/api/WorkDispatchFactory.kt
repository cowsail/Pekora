package org.pekora.api

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Props
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dispatch.core.InlineStepDispatchGateway
import org.pekora.dispatch.core.QueueStepDispatchGateway
import org.pekora.dispatch.core.StepDispatchGateway
import org.pekora.dispatch.core.WorkQueueProvider
import org.pekora.dispatch.pekko.PekkoWorkQueueProvider
import org.pekora.worker.WorkerHost
import org.pekora.worker.WorkerHostConfig
import org.slf4j.LoggerFactory

data class WorkDispatchBootstrap(
    val stepDispatchGateway: StepDispatchGateway,
    val workQueueProvider: WorkQueueProvider? = null,
)

object WorkDispatchFactory {
    private val logger = LoggerFactory.getLogger(WorkDispatchFactory::class.java)

    fun bootstrap(
        system: ActorSystem<*>,
        settings: DistributedWorkersSettings,
    ): WorkDispatchBootstrap {
        if (!settings.enabled || settings.provider == DistributedWorkerProvider.INLINE) {
            return WorkDispatchBootstrap(stepDispatchGateway = InlineStepDispatchGateway())
        }

        val workQueueProvider = when (settings.provider) {
            DistributedWorkerProvider.PEKKO -> PekkoWorkQueueProvider.create(
                system = system,
                actorName = settings.queueActorName,
            )
            DistributedWorkerProvider.INLINE -> null
        }

        if (workQueueProvider == null) {
            return WorkDispatchBootstrap(stepDispatchGateway = InlineStepDispatchGateway())
        }

        return WorkDispatchBootstrap(
            stepDispatchGateway = QueueStepDispatchGateway(
                workQueueProvider = workQueueProvider,
                leaseTimeoutMs = settings.leaseTimeout.toMillis(),
            ),
            workQueueProvider = workQueueProvider,
        )
    }

    fun spawnEmbeddedWorkers(
        context: ActorContext<*>,
        settings: DistributedWorkersSettings,
        workQueueProvider: WorkQueueProvider?,
        agentAdapters: Map<String, AgentRuntimeAdapter>,
        sharding: ClusterSharding,
    ): List<ActorRef<*>> {
        if (workQueueProvider == null || !settings.embeddedWorkers.enabled) {
            return emptyList()
        }

        val replicas = settings.embeddedWorkers.replicas.coerceAtLeast(0)
        return (0 until replicas).map { index ->
            val actorName = "worker-host-${index + 1}"
            val config = workerConfig(settings, index)
            logger.info("Starting embedded worker {} of {} with id {}", index + 1, replicas, config.workerId)
            context.spawn(
                WorkerHost.create(
                    config = config,
                    workQueueProvider = workQueueProvider,
                    agentAdapters = agentAdapters,
                    sharding = sharding,
                ),
                actorName,
            )
        }
    }

    fun spawnEmbeddedWorkers(
        system: ActorSystem<*>,
        settings: DistributedWorkersSettings,
        workQueueProvider: WorkQueueProvider?,
        agentAdapters: Map<String, AgentRuntimeAdapter>,
        sharding: ClusterSharding,
    ): List<ActorRef<*>> {
        if (workQueueProvider == null || !settings.embeddedWorkers.enabled) {
            return emptyList()
        }

        val replicas = settings.embeddedWorkers.replicas.coerceAtLeast(0)
        return (0 until replicas).map { index ->
            val actorName = "worker-host-test-${index + 1}-${settings.embeddedWorkers.workerIdPrefix}"
            val config = workerConfig(settings, index)
            logger.info("Starting embedded worker {} of {} with id {}", index + 1, replicas, config.workerId)
            system.systemActorOf(
                WorkerHost.create(
                    config = config,
                    workQueueProvider = workQueueProvider,
                    agentAdapters = agentAdapters,
                    sharding = sharding,
                ),
                actorName,
                Props.empty(),
            )
        }
    }

    private fun workerConfig(settings: DistributedWorkersSettings, index: Int): WorkerHostConfig =
        WorkerHostConfig(
            workerId = "${settings.embeddedWorkers.workerIdPrefix}-${index + 1}",
            pollInterval = settings.embeddedWorkers.pollInterval,
            maxClaimsPerPoll = settings.embeddedWorkers.maxClaimsPerPoll,
        )
}
