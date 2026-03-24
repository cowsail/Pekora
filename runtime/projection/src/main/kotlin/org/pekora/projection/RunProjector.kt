package org.pekora.projection

import org.pekora.dsl.RunEvent

interface RunEventProjector {
    fun project(event: RunEvent)
}

class DefaultRunEventProjector(
    private val projectionStore: RunProjectionStore,
    private val notificationStore: RunNotificationStore,
) : RunEventProjector {
    override fun project(event: RunEvent) {
        projectionStore.applyEvent(event)
        notificationStore.append(event, projectionStore.getSummary(event.runId))
    }
}
