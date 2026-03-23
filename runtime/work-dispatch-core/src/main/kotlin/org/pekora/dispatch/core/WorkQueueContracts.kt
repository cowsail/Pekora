package org.pekora.dispatch.core

import java.util.concurrent.CompletionStage

data class WorkItem(
    val workItemId: String,
    val runId: String,
    val stepId: String,
    val attempt: Int,
    val backend: String,
    val tenantId: String? = null,
    val capabilityTags: Set<String> = emptySet(),
    val input: Map<String, String>,
    val policySnapshot: Map<String, String>? = null,
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

interface WorkQueueProvider {
    fun enqueue(item: WorkItem): CompletionStage<Unit>
    fun claim(workerId: String, limit: Int): CompletionStage<List<LeasedWorkItem>>
    fun heartbeat(leaseId: String, extendMs: Long): CompletionStage<Boolean>
    fun ack(leaseId: String): CompletionStage<Unit>
    fun release(leaseId: String, reason: String): CompletionStage<Unit>
}
