package org.pekora.projection

import org.pekora.dsl.RunEvent
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicLong

data class RunNotification(
    val sequence: Long,
    val event: RunEvent,
    val summary: RunSummary?,
)

interface RunNotificationStore {
    fun append(event: RunEvent, summary: RunSummary?): RunNotification

    fun readFrom(runId: String, afterSequence: Long? = null, limit: Int = 256): List<RunNotification>

    fun subscribe(runId: String, listener: (RunNotification) -> Unit): AutoCloseable
}

class InMemoryRunNotificationStore : RunNotificationStore {
    private val notificationsByRun = ConcurrentHashMap<String, CopyOnWriteArrayList<RunNotification>>()
    private val subscribersByRun = ConcurrentHashMap<String, CopyOnWriteArrayList<(RunNotification) -> Unit>>()
    private val sequence = AtomicLong(0)

    override fun append(event: RunEvent, summary: RunSummary?): RunNotification {
        val notification = RunNotification(
            sequence = sequence.incrementAndGet(),
            event = event,
            summary = summary,
        )
        notificationsByRun.computeIfAbsent(event.runId) { CopyOnWriteArrayList() }.add(notification)
        subscribersByRun[event.runId]?.forEach { subscriber ->
            subscriber(notification)
        }
        return notification
    }

    override fun readFrom(runId: String, afterSequence: Long?, limit: Int): List<RunNotification> {
        return notificationsByRun[runId]
            ?.asSequence()
            ?.filter { notification -> afterSequence == null || notification.sequence > afterSequence }
            ?.take(limit)
            ?.toList()
            ?: emptyList()
    }

    override fun subscribe(runId: String, listener: (RunNotification) -> Unit): AutoCloseable {
        val subscribers = subscribersByRun.computeIfAbsent(runId) { CopyOnWriteArrayList() }
        subscribers.add(listener)
        return AutoCloseable {
            subscribers.remove(listener)
            if (subscribers.isEmpty()) {
                subscribersByRun.remove(runId, subscribers)
            }
        }
    }
}
