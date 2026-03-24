package org.pekora.dispatch.pekko

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.pekora.dispatch.core.WorkItem
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepKind
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PekkoWorkQueueProviderTest {

    private val testKit = ActorTestKit.create()

    @AfterEach
    fun tearDown() {
        testKit.shutdownTestKit()
    }

    @Test
    fun `enqueue claim heartbeat release and ack`() {
        val provider = PekkoWorkQueueProvider.create(
            system = testKit.system(),
            actorName = "work-queue-${UUID.randomUUID()}",
            askTimeout = Duration.ofSeconds(3),
        )

        provider.enqueue(workItem(stepId = "step-1")).toCompletableFuture().get(3, TimeUnit.SECONDS)
        provider.enqueue(workItem(stepId = "step-2")).toCompletableFuture().get(3, TimeUnit.SECONDS)

        val firstClaim = provider.claim("worker-a", 1).toCompletableFuture().get(3, TimeUnit.SECONDS)
        assertEquals(1, firstClaim.size)
        assertEquals("step-1", firstClaim.first().item.request.stepId)

        val heartbeatOk = provider.heartbeat(firstClaim.first().leaseId, 30_000).toCompletableFuture().get(3, TimeUnit.SECONDS)
        assertTrue(heartbeatOk)

        provider.release(firstClaim.first().leaseId, "test").toCompletableFuture().get(3, TimeUnit.SECONDS)

        val secondClaim = provider.claim("worker-a", 2).toCompletableFuture().get(3, TimeUnit.SECONDS)
        assertEquals(2, secondClaim.size)
        val claimedSteps = secondClaim.map { it.item.request.stepId }.toSet()
        assertEquals(setOf("step-1", "step-2"), claimedSteps)

        secondClaim.forEach { leased ->
            provider.ack(leased.leaseId).toCompletableFuture().get(3, TimeUnit.SECONDS)
        }

        val emptyClaim = provider.claim("worker-a", 2).toCompletableFuture().get(3, TimeUnit.SECONDS)
        assertTrue(emptyClaim.isEmpty())
    }

    @Test
    fun `expired leases are re-queued and reflected in stats`() {
        val provider = PekkoWorkQueueProvider.create(
            system = testKit.system(),
            actorName = "work-queue-${UUID.randomUUID()}",
            askTimeout = Duration.ofSeconds(3),
        )

        provider.enqueue(workItem(stepId = "step-expired", leaseTimeoutMs = 25)).toCompletableFuture().get(3, TimeUnit.SECONDS)

        val claim = provider.claim("worker-a", 1).toCompletableFuture().get(3, TimeUnit.SECONDS)
        assertEquals(1, claim.size)

        Thread.sleep(75)

        val stats = provider.stats().toCompletableFuture().get(3, TimeUnit.SECONDS)
        assertEquals(1, stats.pendingCount)
        assertEquals(0, stats.leasedCount)
        assertEquals(1L, stats.expiredLeaseCount)

        val reclaimed = provider.claim("worker-b", 1).toCompletableFuture().get(3, TimeUnit.SECONDS)
        assertEquals(1, reclaimed.size)
        assertEquals("step-expired", reclaimed.first().item.request.stepId)
    }

    private fun workItem(stepId: String, leaseTimeoutMs: Long = 60_000): WorkItem =
        WorkItem(
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
            leaseTimeoutMs = leaseTimeoutMs,
            maxAttempts = 1,
        )
}
