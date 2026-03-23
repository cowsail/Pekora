package org.pekora.dispatch.pekko

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.Props
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import org.pekora.dispatch.core.LeasedWorkItem
import org.pekora.dispatch.core.WorkItem
import org.pekora.dispatch.core.WorkQueueProvider
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletionStage

class PekkoWorkQueueProvider private constructor(
    private val queueActor: ActorRef<WorkQueueCommand>,
    private val scheduler: Scheduler,
    private val askTimeout: Duration = Duration.ofSeconds(3),
) : WorkQueueProvider {
    companion object {
        fun create(
            system: ActorSystem<*>,
            actorName: String = "work-queue",
            askTimeout: Duration = Duration.ofSeconds(3),
        ): PekkoWorkQueueProvider {
            val actor = system.systemActorOf(PekkoWorkQueue.create(), actorName, Props.empty())
            return PekkoWorkQueueProvider(actor, system.scheduler(), askTimeout)
        }
    }

    override fun enqueue(item: WorkItem): CompletionStage<Unit> =
        AskPattern.ask(queueActor, { replyTo -> Enqueue(item, replyTo) }, askTimeout, scheduler)
            .thenApply { Unit }

    override fun claim(workerId: String, limit: Int): CompletionStage<List<LeasedWorkItem>> =
        AskPattern.ask(queueActor, { replyTo -> Claim(workerId, limit, replyTo) }, askTimeout, scheduler)
            .thenApply { response -> response.items }

    override fun heartbeat(leaseId: String, extendMs: Long): CompletionStage<Boolean> =
        AskPattern.ask(queueActor, { replyTo -> Heartbeat(leaseId, extendMs, replyTo) }, askTimeout, scheduler)
            .thenApply { response -> response.success }

    override fun ack(leaseId: String): CompletionStage<Unit> =
        AskPattern.ask(queueActor, { replyTo -> Ack(leaseId, replyTo) }, askTimeout, scheduler)
            .thenApply { Unit }

    override fun release(leaseId: String, reason: String): CompletionStage<Unit> =
        AskPattern.ask(queueActor, { replyTo -> Release(leaseId, reason, replyTo) }, askTimeout, scheduler)
            .thenApply { Unit }
}

internal sealed interface WorkQueueCommand

private data class Enqueue(
    val item: WorkItem,
    val replyTo: ActorRef<QueueAck>,
) : WorkQueueCommand

private data class Claim(
    val workerId: String,
    val limit: Int,
    val replyTo: ActorRef<ClaimedItems>,
) : WorkQueueCommand

private data class Heartbeat(
    val leaseId: String,
    val extendMs: Long,
    val replyTo: ActorRef<HeartbeatAck>,
) : WorkQueueCommand

private data class Ack(
    val leaseId: String,
    val replyTo: ActorRef<QueueAck>,
) : WorkQueueCommand

private data class Release(
    val leaseId: String,
    val reason: String,
    val replyTo: ActorRef<QueueAck>,
) : WorkQueueCommand

private data class QueueAck(val accepted: Boolean = true)
private data class HeartbeatAck(val success: Boolean)
private data class ClaimedItems(val items: List<LeasedWorkItem>)

private class PekkoWorkQueue(
    context: ActorContext<WorkQueueCommand>,
) : AbstractBehavior<WorkQueueCommand>(context) {

    companion object {
        fun create(): Behavior<WorkQueueCommand> = Behaviors.setup(::PekkoWorkQueue)
    }

    private data class LeaseRecord(
        val leaseId: String,
        val workerId: String,
        val workItem: WorkItem,
        var leasedUntilEpochMs: Long,
    )

    private val pending = ArrayDeque<WorkItem>()
    private val leasesById = mutableMapOf<String, LeaseRecord>()

    override fun createReceive(): Receive<WorkQueueCommand> =
        newReceiveBuilder()
            .onMessage(Enqueue::class.java, this::onEnqueue)
            .onMessage(Claim::class.java, this::onClaim)
            .onMessage(Heartbeat::class.java, this::onHeartbeat)
            .onMessage(Ack::class.java, this::onAck)
            .onMessage(Release::class.java, this::onRelease)
            .build()

    private fun onEnqueue(msg: Enqueue): Behavior<WorkQueueCommand> {
        pending.addLast(msg.item)
        msg.replyTo.tell(QueueAck())
        return this
    }

    private fun onClaim(msg: Claim): Behavior<WorkQueueCommand> {
        reclaimExpiredLeases()
        val now = System.currentTimeMillis()
        val leased = mutableListOf<LeasedWorkItem>()
        repeat(msg.limit.coerceAtLeast(0)) {
            val item = pending.removeFirstOrNull() ?: return@repeat
            val leaseId = UUID.randomUUID().toString()
            val lease = LeaseRecord(
                leaseId = leaseId,
                workerId = msg.workerId,
                workItem = item,
                leasedUntilEpochMs = now + item.leaseTimeoutMs,
            )
            leasesById[leaseId] = lease
            leased.add(
                LeasedWorkItem(
                    leaseId = leaseId,
                    workerId = msg.workerId,
                    leasedUntilEpochMs = lease.leasedUntilEpochMs,
                    item = item,
                )
            )
        }
        msg.replyTo.tell(ClaimedItems(leased))
        return this
    }

    private fun onHeartbeat(msg: Heartbeat): Behavior<WorkQueueCommand> {
        reclaimExpiredLeases()
        val lease = leasesById[msg.leaseId]
        if (lease == null) {
            msg.replyTo.tell(HeartbeatAck(false))
            return this
        }
        lease.leasedUntilEpochMs = System.currentTimeMillis() + msg.extendMs
        msg.replyTo.tell(HeartbeatAck(true))
        return this
    }

    private fun onAck(msg: Ack): Behavior<WorkQueueCommand> {
        leasesById.remove(msg.leaseId)
        msg.replyTo.tell(QueueAck())
        return this
    }

    private fun onRelease(msg: Release): Behavior<WorkQueueCommand> {
        val lease = leasesById.remove(msg.leaseId)
        if (lease != null) {
            pending.addLast(lease.workItem)
        }
        msg.replyTo.tell(QueueAck())
        return this
    }

    private fun reclaimExpiredLeases() {
        val now = System.currentTimeMillis()
        val expired = leasesById.values.filter { it.leasedUntilEpochMs <= now }
        if (expired.isEmpty()) {
            return
        }
        expired.forEach { lease ->
            leasesById.remove(lease.leaseId)
            pending.addFirst(lease.workItem)
        }
    }
}
