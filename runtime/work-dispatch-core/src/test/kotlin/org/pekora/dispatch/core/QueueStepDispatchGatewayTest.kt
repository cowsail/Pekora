package org.pekora.dispatch.core

import org.junit.jupiter.api.Test
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepKind
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class QueueStepDispatchGatewayTest {

    @Test
    fun `dispatch enqueues work item with attempt metadata`() {
        val fakeQueue = RecordingQueueProvider()
        val gateway = QueueStepDispatchGateway(
            workQueueProvider = fakeQueue,
            leaseTimeoutMs = 45_000,
        )

        val decision = gateway.dispatch(
            StepDispatchRequest(
                request = request(stepId = "step-a"),
                attempt = 2,
                maxAttempts = 5,
            )
        ).toCompletableFuture().join()

        val enqueued = fakeQueue.lastEnqueued
        assertNotNull(enqueued)
        assertEquals(2, enqueued.attempt)
        assertEquals(5, enqueued.maxAttempts)
        assertEquals("step-a", enqueued.request.stepId)
        assertEquals(45_000, enqueued.leaseTimeoutMs)
        assertEquals(DispatchMode.QUEUE, decision.mode)
    }

    private fun request(stepId: String) = StepExecutionRequest(
        runId = "run-1",
        stepId = stepId,
        stepKind = StepKind.AGENT,
        backend = "langgraph",
        input = mapOf("prompt" to "hello"),
    )
}

private class RecordingQueueProvider : WorkQueueProvider {
    var lastEnqueued: WorkItem? = null

    override fun enqueue(item: WorkItem): CompletionStage<Unit> {
        lastEnqueued = item
        return CompletableFuture.completedFuture(Unit)
    }

    override fun claim(workerId: String, limit: Int): CompletionStage<List<LeasedWorkItem>> =
        CompletableFuture.completedFuture(emptyList())

    override fun heartbeat(leaseId: String, extendMs: Long): CompletionStage<Boolean> =
        CompletableFuture.completedFuture(false)

    override fun ack(leaseId: String): CompletionStage<Unit> =
        CompletableFuture.completedFuture(Unit)

    override fun release(leaseId: String, reason: String): CompletionStage<Unit> =
        CompletableFuture.completedFuture(Unit)
}
