package org.pekora.dispatch.core

import org.pekora.dsl.StepExecutionRequest
import java.util.concurrent.CompletionStage

data class WorkItem(
    val workItemId: String,
    val attempt: Int,
    val tenantId: String? = null,
    val capabilityTags: Set<String> = emptySet(),
    val policySnapshot: Map<String, String>? = null,
    val request: StepExecutionRequest,
    val createdAtEpochMs: Long,
    val leaseTimeoutMs: Long,
    val maxAttempts: Int,
)

data class LeasedWorkItem(
    val leaseId: String,
    val workerId: String,
    val leasedUntilEpochMs: Long,
    val item: WorkItem,
)

data class WorkQueueStats(
    val pendingCount: Int,
    val leasedCount: Int,
    val expiredLeaseCount: Long,
)

interface WorkQueueProvider {
    fun enqueue(item: WorkItem): CompletionStage<Unit>
    fun claim(workerId: String, limit: Int): CompletionStage<List<LeasedWorkItem>>
    fun heartbeat(leaseId: String, extendMs: Long): CompletionStage<Boolean>
    fun ack(leaseId: String): CompletionStage<Unit>
    fun release(leaseId: String, reason: String): CompletionStage<Unit>
    fun stats(): CompletionStage<WorkQueueStats>
}
