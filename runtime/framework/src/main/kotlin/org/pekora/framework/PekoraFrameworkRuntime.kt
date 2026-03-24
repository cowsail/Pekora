package org.pekora.framework

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.engine.ApprovalCommand
import org.pekora.engine.StepExecutorMessage
import org.pekora.projection.RunNotificationStore
import org.pekora.projection.RunProjectionStore
import org.pekora.registry.RegistryCommand

data class PekoraFrameworkRuntime(
    val system: ActorSystem<*>,
    val registry: ActorRef<RegistryCommand>,
    val approvalManager: ActorRef<ApprovalCommand>,
    val sharding: ClusterSharding,
    val stepExecutor: ActorRef<StepExecutorMessage>,
    val runProjection: RunProjectionStore,
    val runNotifications: RunNotificationStore,
    val agentAdapters: Map<String, AgentRuntimeAdapter>,
    val distributedWorkers: DistributedWorkersSettings,
)
