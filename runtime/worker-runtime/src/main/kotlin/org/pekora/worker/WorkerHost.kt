package org.pekora.worker

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import org.apache.pekko.actor.typed.javadsl.TimerScheduler
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dispatch.core.LeasedWorkItem
import org.pekora.dispatch.core.WorkQueueProvider
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.pekora.engine.RunEntityTypeKey
import org.pekora.engine.StepResult
import org.slf4j.LoggerFactory
import java.time.Duration

data class WorkerHostConfig(
    val workerId: String,
    val pollInterval: Duration = Duration.ofMillis(250),
    val maxClaimsPerPoll: Int = 8,
)

sealed interface WorkerHostMessage
data object PollTick : WorkerHostMessage
data class ClaimedInternal(
    val items: List<LeasedWorkItem>? = null,
    val throwable: Throwable? = null,
) : WorkerHostMessage
data class ExecuteLease(
    val leased: LeasedWorkItem,
) : WorkerHostMessage
data class ExecutionDoneInternal(
    val leased: LeasedWorkItem,
    val result: StepExecutionResult? = null,
    val throwable: Throwable? = null,
) : WorkerHostMessage

class WorkerHost(
    context: ActorContext<WorkerHostMessage>,
    timers: TimerScheduler<WorkerHostMessage>,
    private val config: WorkerHostConfig,
    private val workQueueProvider: WorkQueueProvider,
    private val agentAdapters: Map<String, AgentRuntimeAdapter>,
    private val sharding: ClusterSharding,
) : AbstractBehavior<WorkerHostMessage>(context) {

    companion object {
        private val logger = LoggerFactory.getLogger(WorkerHost::class.java)

        fun create(
            config: WorkerHostConfig,
            workQueueProvider: WorkQueueProvider,
            agentAdapters: Map<String, AgentRuntimeAdapter>,
            sharding: ClusterSharding,
        ): Behavior<WorkerHostMessage> = Behaviors.withTimers { timers ->
            Behaviors.setup { context ->
                timers.startSingleTimer(PollTick, PollTick, Duration.ZERO)
                WorkerHost(context, timers, config, workQueueProvider, agentAdapters, sharding)
            }
        }
    }

    private val timers = timers

    override fun createReceive(): Receive<WorkerHostMessage> =
        newReceiveBuilder()
            .onMessage(PollTick::class.java) { onPollTick() }
            .onMessage(ClaimedInternal::class.java, this::onClaimedInternal)
            .onMessage(ExecuteLease::class.java, this::onExecuteLease)
            .onMessage(ExecutionDoneInternal::class.java, this::onExecutionDone)
            .build()

    private fun onPollTick(): Behavior<WorkerHostMessage> {
        context.pipeToSelf(
            workQueueProvider.claim(config.workerId, config.maxClaimsPerPoll)
        ) { items, throwable ->
            if (throwable != null) {
                ClaimedInternal(throwable = throwable)
            } else {
                ClaimedInternal(items = items)
            }
        }
        scheduleNextPoll()
        return this
    }

    private fun onClaimedInternal(msg: ClaimedInternal): Behavior<WorkerHostMessage> {
        val throwable = msg.throwable
        if (throwable != null) {
            logger.warn("Worker '{}' failed to claim work: {}", config.workerId, throwable.message)
            return this
        }

        msg.items.orEmpty().forEach { leased ->
            context.self.tell(ExecuteLease(leased))
        }
        return this
    }

    private fun onExecuteLease(msg: ExecuteLease): Behavior<WorkerHostMessage> {
        val request = msg.leased.item.request
        val adapter = agentAdapters[request.backend]
        if (adapter == null) {
            val result = StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "No adapter found for backend: ${request.backend}",
            )
            context.self.tell(ExecutionDoneInternal(leased = msg.leased, result = result))
            return this
        }

        context.pipeToSelf(adapter.executeStep(request)) { result, throwable ->
            if (throwable != null) {
                ExecutionDoneInternal(leased = msg.leased, throwable = throwable)
            } else {
                ExecutionDoneInternal(leased = msg.leased, result = result)
            }
        }
        return this
    }

    private fun onExecutionDone(msg: ExecutionDoneInternal): Behavior<WorkerHostMessage> {
        val request = msg.leased.item.request
        val result = msg.result ?: StepExecutionResult(
            status = StepResultStatus.FAILED,
            error = msg.throwable?.message ?: "Unknown worker execution error",
        )

        val runRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, request.runId)
        runRef.tell(StepResult(stepId = request.stepId, attempt = msg.leased.item.attempt, result = result))

        workQueueProvider.ack(msg.leased.leaseId).whenComplete { _, ackError ->
            if (ackError != null) {
                logger.warn(
                    "Worker '{}' failed to ack lease '{}' for step '{}': {}",
                    config.workerId,
                    msg.leased.leaseId,
                    request.stepId,
                    ackError.message,
                )
            }
        }
        return this
    }

    private fun scheduleNextPoll() {
        timers.startSingleTimer(PollTick, PollTick, config.pollInterval)
    }
}
