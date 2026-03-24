package org.pekora.projection

import org.pekora.dsl.ApprovalReceived
import org.pekora.dsl.ApprovalRequested
import org.pekora.dsl.ExecutionMetrics
import org.pekora.dsl.ParallelBranchCompleted
import org.pekora.dsl.ParallelBranchFailed
import org.pekora.dsl.ParallelGroupCompleted
import org.pekora.dsl.ParallelGroupFailed
import org.pekora.dsl.ParallelGroupStarted
import org.pekora.dsl.RunCancelled
import org.pekora.dsl.RunCompleted
import org.pekora.dsl.RunCreated
import org.pekora.dsl.RunEvent
import org.pekora.dsl.RunFailed
import org.pekora.dsl.RunStarted
import org.pekora.dsl.RunState
import org.pekora.dsl.StepCompleted
import org.pekora.dsl.StepFailed
import org.pekora.dsl.StepScheduled
import org.pekora.dsl.StepStarted
import org.pekora.dsl.StepState
import org.pekora.dsl.SubworkflowChildCompleted
import org.pekora.dsl.SubworkflowChildFailed
import org.pekora.dsl.SubworkflowChildStarted
import org.pekora.dsl.ToolCallRecord
import org.pekora.dsl.WorkflowLoaded
import java.util.concurrent.ConcurrentHashMap

data class RunSummary(
    val runId: String,
    val templateId: String,
    val version: Int,
    val status: RunState,
    val stepStates: Map<String, StepState>,
    val startedAt: Long? = null,
    val completedAt: Long? = null,
    val error: String? = null,
    val tenantId: String = "",
)

data class RunTimelineEntry(
    val timestamp: Long,
    val eventType: String,
    val stepId: String? = null,
    val detail: String = "",
)

data class RunTimeline(
    val runId: String,
    val entries: List<RunTimelineEntry>,
)

interface RunProjectionStore {
    fun applyEvent(event: RunEvent)

    fun getSummary(runId: String): RunSummary?

    fun getTimeline(runId: String): RunTimeline

    fun listRuns(tenantId: String? = null): List<RunSummary>

    fun getToolCalls(runId: String): Map<String, List<ToolCallRecord>>

    fun getStepOutput(runId: String, stepId: String): Map<String, String>?

    fun getActiveRuns(): List<RunSummary>
}

class InMemoryRunProjectionStore : RunProjectionStore {
    private val summaries = ConcurrentHashMap<String, RunSummary>()
    private val timelines = ConcurrentHashMap<String, MutableList<RunTimelineEntry>>()
    private val toolCallsByStep = ConcurrentHashMap<String, MutableMap<String, List<ToolCallRecord>>>()
    private val stepOutputsByRun = ConcurrentHashMap<String, MutableMap<String, Map<String, String>>>()

    override fun applyEvent(event: RunEvent) {
        when (event) {
            is RunCreated -> {
                summaries[event.runId] = RunSummary(
                    runId = event.runId,
                    templateId = event.templateId,
                    version = event.versionNumber,
                    status = RunState.CREATED,
                    stepStates = emptyMap(),
                    tenantId = event.tenantId,
                )
                addTimelineEntry(event.runId, event.timestamp, "RunCreated", detail = "Run created for ${event.templateId}:${event.versionNumber}")
            }
            is RunStarted -> {
                updateSummary(event.runId) { it.copy(status = RunState.EXECUTING, startedAt = event.timestamp) }
                addTimelineEntry(event.runId, event.timestamp, "RunStarted")
            }
            is StepStarted -> {
                updateSummary(event.runId) {
                    it.copy(stepStates = it.stepStates + (event.stepId to StepState.RUNNING))
                }
                addTimelineEntry(event.runId, event.timestamp, "StepStarted", event.stepId, "Backend: ${event.backend}")
            }
            is StepCompleted -> {
                updateSummary(event.runId) {
                    it.copy(stepStates = it.stepStates + (event.stepId to StepState.SUCCEEDED))
                }
                stepOutputsByRun.computeIfAbsent(event.runId) { mutableMapOf() }[event.stepId] = event.output
                val toolDetail = if (event.toolCalls.isNotEmpty()) {
                    ", tools: ${event.toolCalls.joinToString { it.tool }}"
                } else {
                    ""
                }
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "StepCompleted",
                    event.stepId,
                    "Duration: ${event.metrics.durationMs}ms$toolDetail",
                )
                if (event.toolCalls.isNotEmpty()) {
                    toolCallsByStep.computeIfAbsent(event.runId) { mutableMapOf() }[event.stepId] = event.toolCalls
                }
            }
            is ParallelGroupStarted -> {
                updateSummary(event.runId) {
                    it.copy(stepStates = it.stepStates + (event.parallelStepId to StepState.RUNNING))
                }
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "ParallelGroupStarted",
                    event.parallelStepId,
                    "Branches: ${event.branches.joinToString(", ")}",
                )
            }
            is ParallelBranchCompleted -> {
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "ParallelBranchCompleted",
                    event.parallelStepId,
                    "Branch root: ${event.branchRootStepId}",
                )
            }
            is ParallelBranchFailed -> {
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "ParallelBranchFailed",
                    event.parallelStepId,
                    "Branch root: ${event.branchRootStepId}, error: ${event.error}",
                )
            }
            is ParallelGroupCompleted -> {
                updateSummary(event.runId) {
                    it.copy(stepStates = it.stepStates + (event.parallelStepId to StepState.SUCCEEDED))
                }
                stepOutputsByRun.computeIfAbsent(event.runId) { mutableMapOf() }[event.parallelStepId] = event.output
                addTimelineEntry(event.runId, event.timestamp, "ParallelGroupCompleted", event.parallelStepId)
            }
            is ParallelGroupFailed -> {
                updateSummary(event.runId) {
                    it.copy(stepStates = it.stepStates + (event.parallelStepId to StepState.FAILED))
                }
                addTimelineEntry(event.runId, event.timestamp, "ParallelGroupFailed", event.parallelStepId, event.error)
            }
            is StepFailed -> {
                updateSummary(event.runId) {
                    it.copy(stepStates = it.stepStates + (event.stepId to StepState.FAILED))
                }
                addTimelineEntry(event.runId, event.timestamp, "StepFailed", event.stepId, event.error)
            }
            is SubworkflowChildStarted -> {
                updateSummary(event.runId) {
                    it.copy(stepStates = it.stepStates + (event.stepId to StepState.RUNNING))
                }
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "SubworkflowChildStarted",
                    event.stepId,
                    "Child run: ${event.childRunId}",
                )
            }
            is SubworkflowChildCompleted -> {
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "SubworkflowChildCompleted",
                    event.stepId,
                    "Child run: ${event.childRunId}",
                )
            }
            is SubworkflowChildFailed -> {
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "SubworkflowChildFailed",
                    event.stepId,
                    "Child run: ${event.childRunId}, error: ${event.error}",
                )
            }
            is ApprovalRequested -> {
                updateSummary(event.runId) {
                    it.copy(
                        status = RunState.WAITING_FOR_APPROVAL,
                        stepStates = it.stepStates + (event.stepId to StepState.BLOCKED),
                    )
                }
                addTimelineEntry(event.runId, event.timestamp, "ApprovalRequested", event.stepId, "Approval ID: ${event.approvalId}")
            }
            is ApprovalReceived -> {
                val newState = if (event.approved) StepState.SUCCEEDED else StepState.CANCELLED
                updateSummary(event.runId) {
                    it.copy(
                        status = RunState.EXECUTING,
                        stepStates = it.stepStates + (event.stepId to newState),
                    )
                }
                addTimelineEntry(
                    event.runId,
                    event.timestamp,
                    "ApprovalReceived",
                    event.stepId,
                    "${if (event.approved) "Approved" else "Rejected"} by ${event.approver}",
                )
            }
            is RunCompleted -> {
                updateSummary(event.runId) {
                    it.copy(status = RunState.COMPLETED, completedAt = event.timestamp)
                }
                addTimelineEntry(event.runId, event.timestamp, "RunCompleted")
            }
            is RunFailed -> {
                updateSummary(event.runId) {
                    it.copy(status = RunState.FAILED, completedAt = event.timestamp, error = event.error)
                }
                addTimelineEntry(event.runId, event.timestamp, "RunFailed", detail = event.error)
            }
            is RunCancelled -> {
                updateSummary(event.runId) {
                    it.copy(status = RunState.CANCELLED, completedAt = event.timestamp)
                }
                addTimelineEntry(event.runId, event.timestamp, "RunCancelled", detail = event.reason)
            }
            else -> {
                addTimelineEntry(event.runId, event.timestamp, event::class.simpleName ?: "Unknown")
            }
        }
    }

    override fun getSummary(runId: String): RunSummary? = summaries[runId]

    override fun getTimeline(runId: String): RunTimeline {
        val entries = timelines[runId] ?: emptyList()
        return RunTimeline(runId, entries.toList())
    }

    override fun listRuns(tenantId: String?): List<RunSummary> {
        return if (tenantId != null) {
            summaries.values.filter { it.tenantId == tenantId }
        } else {
            summaries.values.toList()
        }
    }

    override fun getToolCalls(runId: String): Map<String, List<ToolCallRecord>> {
        return toolCallsByStep[runId]?.toMap() ?: emptyMap()
    }

    override fun getStepOutput(runId: String, stepId: String): Map<String, String>? {
        return stepOutputsByRun[runId]?.get(stepId)
    }

    override fun getActiveRuns(): List<RunSummary> {
        return summaries.values.filter {
            it.status in listOf(RunState.EXECUTING, RunState.WAITING_FOR_APPROVAL, RunState.WAITING_FOR_EXTERNAL_EVENT)
        }
    }

    private fun updateSummary(runId: String, update: (RunSummary) -> RunSummary) {
        summaries.computeIfPresent(runId) { _, current -> update(current) }
    }

    private fun addTimelineEntry(
        runId: String,
        timestamp: Long,
        eventType: String,
        stepId: String? = null,
        detail: String = "",
    ) {
        timelines.computeIfAbsent(runId) { mutableListOf() }
            .add(RunTimelineEntry(timestamp, eventType, stepId, detail))
    }
}
