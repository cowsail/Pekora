package org.pekora.worker

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dispatch.core.LeasedWorkItem
import org.pekora.dispatch.core.StepResultSink
import org.pekora.dispatch.core.WorkItem
import org.pekora.dispatch.core.WorkQueueProvider
import org.pekora.dispatch.core.WorkQueueStats
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepKind
import org.pekora.dsl.StepResultStatus
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class WorkerHostTest {

    private val testKit = ActorTestKit.create()

    @AfterEach
    fun tearDown() {
        testKit.shutdownTestKit()
    }

    @Test
    fun `worker acks lease after successful result submission`() {
        val queue = RecordingQueueProvider(claimedItems = listOf(leasedItem()))
        val sink = RecordingResultSink()
        testKit.spawn(
            WorkerHost.create(
                config = WorkerHostConfig(workerId = "worker-1", pollInterval = Duration.ofSeconds(5), maxClaimsPerPoll = 1),
                workQueueProvider = queue,
                agentAdapters = mapOf("langgraph" to SuccessAdapter()),
                stepResultSink = sink,
            )
        )

        waitForCondition { sink.submissions.get() == 1 }
        waitForCondition { queue.acks.get() == 1 }
        assertEquals(0, queue.releases.get())
    }

    @Test
    fun `worker releases lease when result submission fails`() {
        val queue = RecordingQueueProvider(claimedItems = listOf(leasedItem(stepId = "step-2")))
        val sink = FailingResultSink()
        testKit.spawn(
            WorkerHost.create(
                config = WorkerHostConfig(workerId = "worker-2", pollInterval = Duration.ofSeconds(5), maxClaimsPerPoll = 1),
                workQueueProvider = queue,
                agentAdapters = mapOf("langgraph" to SuccessAdapter()),
                stepResultSink = sink,
            )
        )

        waitForCondition { queue.releases.get() == 1 }
        assertEquals(0, queue.acks.get())
        assertTrue(queue.releaseReasons.contains("result submission failed"))
    }

    private fun leasedItem(stepId: String = "step-1"): LeasedWorkItem =
        LeasedWorkItem(
            leaseId = UUID.randomUUID().toString(),
            workerId = "worker-test",
            leasedUntilEpochMs = System.currentTimeMillis() + 60_000,
            item = WorkItem(
                workItemId = UUID.randomUUID().toString(),
                attempt = 1,
                request = StepExecutionRequest(
                    runId = "run-1",
                    stepId = stepId,
                    stepKind = StepKind.AGENT,
                    backend = "langgraph",
                    input = mapOf("prompt" to "hello"),
                ),
                createdAtEpochMs = System.currentTimeMillis(),
                leaseTimeoutMs = 60_000,
                maxAttempts = 2,
            ),
        )

    private fun waitForCondition(timeout: Duration = Duration.ofSeconds(5), predicate: () -> Boolean) {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (System.nanoTime() < deadline) {
            if (predicate()) {
                return
            }
            Thread.sleep(25)
        }
        throw AssertionError("Timed out waiting for condition")
    }
}

private class RecordingQueueProvider(
    claimedItems: List<LeasedWorkItem>,
) : WorkQueueProvider {
    private val remaining = ArrayDeque(claimedItems)
    val acks = AtomicInteger(0)
    val releases = AtomicInteger(0)
    val releaseReasons = mutableListOf<String>()

    override fun enqueue(item: WorkItem): CompletionStage<Unit> =
        CompletableFuture.completedFuture(Unit)

    override fun claim(workerId: String, limit: Int): CompletionStage<List<LeasedWorkItem>> {
        val claimed = mutableListOf<LeasedWorkItem>()
        repeat(limit) {
            val next = remaining.removeFirstOrNull() ?: return@repeat
            claimed.add(next)
        }
        return CompletableFuture.completedFuture(claimed)
    }

    override fun heartbeat(leaseId: String, extendMs: Long): CompletionStage<Boolean> =
        CompletableFuture.completedFuture(true)

    override fun ack(leaseId: String): CompletionStage<Unit> {
        acks.incrementAndGet()
        return CompletableFuture.completedFuture(Unit)
    }

    override fun release(leaseId: String, reason: String): CompletionStage<Unit> {
        releases.incrementAndGet()
        releaseReasons.add(reason)
        return CompletableFuture.completedFuture(Unit)
    }

    override fun stats(): CompletionStage<WorkQueueStats> =
        CompletableFuture.completedFuture(
            WorkQueueStats(
                pendingCount = remaining.size,
                leasedCount = 0,
                expiredLeaseCount = 0,
            )
        )
}

private class RecordingResultSink : StepResultSink {
    val submissions = AtomicInteger(0)

    override fun submit(
        runId: String,
        stepId: String,
        attempt: Int,
        result: StepExecutionResult,
    ): CompletionStage<Unit> {
        submissions.incrementAndGet()
        return CompletableFuture.completedFuture(Unit)
    }
}

private class FailingResultSink : StepResultSink {
    override fun submit(
        runId: String,
        stepId: String,
        attempt: Int,
        result: StepExecutionResult,
    ): CompletionStage<Unit> =
        CompletableFuture.failedFuture(IllegalStateException("sink unavailable"))
}

private class SuccessAdapter : AgentRuntimeAdapter {
    override val backendId: String = "langgraph"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> =
        CompletableFuture.completedFuture(
            StepExecutionResult(
                status = StepResultStatus.SUCCEEDED,
                output = mapOf("answer" to "ok"),
            )
        )
}
