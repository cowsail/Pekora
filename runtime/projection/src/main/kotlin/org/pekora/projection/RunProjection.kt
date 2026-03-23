/**
 * Read-model projections for workflow run status and timeline (Section 10.4, Section 16.3).
 *
 * This file provides the in-memory projection store that maintains a queryable read model
 * of all workflow runs. It consumes [RunEvent] instances emitted by the run entity and
 * projects them into two structures:
 *
 * - **[RunSummary]**: a snapshot of the current state of a run (status, step states,
 *   timestamps, and error information).
 * - **[RunTimeline]**: an ordered list of [RunTimelineEntry] records that capture
 *   every significant event in the run's lifecycle, suitable for audit trails and
 *   debugging.
 *
 * **v1 design**: The current implementation stores all data in [ConcurrentHashMap]
 * instances. In a future version, this can be backed by a database and driven by
 * Pekko Projections for event-sourced, eventually-consistent read models.
 *
 * @see RunProjectionStore
 * @see RunSummary
 * @see RunTimeline
 */
package org.pekora.projection

import org.pekora.dsl.*
import java.util.concurrent.ConcurrentHashMap



/**
 * A denormalized snapshot of a workflow run's current state (Section 10.4).
 *
 * Maintained by [RunProjectionStore] as events are applied. This is the primary
 * data structure returned by status queries and listing endpoints.
 *
 * @property runId The unique identifier of the run.
 * @property templateId The identifier of the workflow template this run was created from.
 * @property version The version number of the workflow definition used for this run.
 * @property status The current high-level [RunState] of the run (e.g., CREATED, EXECUTING,
 *   COMPLETED, FAILED).
 * @property stepStates A map from step identifiers to their current [StepState], reflecting
 *   per-step progress within the run.
 * @property startedAt Epoch-millisecond timestamp of when the run transitioned to EXECUTING,
 *   or `null` if not yet started.
 * @property completedAt Epoch-millisecond timestamp of when the run reached a terminal state
 *   (COMPLETED, FAILED, or CANCELLED), or `null` if still active.
 * @property error A human-readable error message if the run has failed, or `null` otherwise.
 * @property tenantId The tenant this run belongs to, used for multi-tenant filtering.
 * @see RunProjectionStore
 * @see RunState
 * @see StepState
 */
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

/**
 * A single entry in a run's timeline, representing one significant lifecycle event
 * (Section 16.3).
 *
 * Timeline entries are appended in chronological order and are never mutated after
 * creation.
 *
 * @property timestamp Epoch-millisecond timestamp of when the event occurred.
 * @property eventType A short label identifying the event kind (e.g., `"RunStarted"`,
 *   `"StepCompleted"`, `"ApprovalRequested"`).
 * @property stepId The identifier of the step this event relates to, or `null` for
 *   run-level events.
 * @property detail Additional human-readable context (e.g., duration, error message,
 *   approval ID).
 * @see RunTimeline
 */
data class RunTimelineEntry(
    val timestamp: Long,
    val eventType: String,
    val stepId: String? = null,
    val detail: String = "",
)

/**
 * The complete timeline for a single workflow run, composed of ordered
 * [RunTimelineEntry] records.
 *
 * @property runId The unique identifier of the run this timeline belongs to.
 * @property entries The chronologically ordered list of timeline entries.
 * @see RunTimelineEntry
 * @see RunProjectionStore.getTimeline
 */
data class RunTimeline(
    val runId: String,
    val entries: List<RunTimelineEntry>,
)

/**
 * In-memory projection store for workflow run read models (Section 10.4, Section 16.3).
 *
 * The store accepts [RunEvent] instances via [applyEvent] and maintains two concurrent
 * data structures:
 *
 * - A [ConcurrentHashMap] of [RunSummary] objects keyed by run ID, providing O(1)
 *   lookup of current run state.
 * - A [ConcurrentHashMap] of mutable timeline lists keyed by run ID, capturing the
 *   full ordered history of events for audit and debugging.
 *
 * Thread safety is ensured by [ConcurrentHashMap.computeIfPresent] and
 * [ConcurrentHashMap.computeIfAbsent], making the store safe for concurrent reads
 * and writes from multiple actors or threads.
 *
 * @see RunSummary
 * @see RunTimeline
 * @see RunEvent
 */
class RunProjectionStore {

    private val summaries = ConcurrentHashMap<String, RunSummary>()
    private val timelines = ConcurrentHashMap<String, MutableList<RunTimelineEntry>>()
    // runId -> stepId -> list of tool calls recorded from StepCompleted events
    private val toolCallsByStep = ConcurrentHashMap<String, MutableMap<String, List<ToolCallRecord>>>()

    /**
     * Applies a [RunEvent] to the projection, updating the corresponding [RunSummary]
     * and appending a [RunTimelineEntry].
     *
     * The method pattern-matches on the concrete event type and delegates to the
     * appropriate state transition:
     *
     * - [RunCreated]: initializes a new summary in CREATED state.
     * - [RunStarted]: transitions to EXECUTING and records the start timestamp.
     * - [StepStarted] / [StepCompleted] / [StepFailed]: updates per-step state.
     * - [ApprovalRequested] / [ApprovalReceived]: manages the approval lifecycle.
     * - [RunCompleted] / [RunFailed] / [RunCancelled]: transitions to a terminal state.
     * - Any other event: records a timeline entry without modifying the summary.
     *
     * @param event The run event to project into the read model.
     * @see RunEvent
     * @see RunSummary
     */
    fun applyEvent(event: RunEvent) {
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
                val toolDetail = if (event.toolCalls.isNotEmpty()) {
                    ", tools: ${event.toolCalls.joinToString { it.tool }}"
                } else ""
                addTimelineEntry(event.runId, event.timestamp, "StepCompleted", event.stepId,
                    "Duration: ${event.metrics.durationMs}ms$toolDetail")
                if (event.toolCalls.isNotEmpty()) {
                    toolCallsByStep
                        .computeIfAbsent(event.runId) { mutableMapOf() }[event.stepId] = event.toolCalls
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
                addTimelineEntry(event.runId, event.timestamp, "ApprovalReceived", event.stepId,
                    "${if (event.approved) "Approved" else "Rejected"} by ${event.approver}")
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

    /**
     * Retrieves the current [RunSummary] for the given run.
     *
     * @param runId The unique identifier of the run to look up.
     * @return The [RunSummary] if the run exists in the projection, or `null` if no
     *   events have been projected for that run ID.
     * @see RunSummary
     */
    fun getSummary(runId: String): RunSummary? = summaries[runId]

    /**
     * Retrieves the full [RunTimeline] for the given run.
     *
     * @param runId The unique identifier of the run whose timeline is requested.
     * @return A [RunTimeline] containing all projected timeline entries in chronological
     *   order. If the run ID is unknown, returns a timeline with an empty entry list.
     * @see RunTimeline
     * @see RunTimelineEntry
     */
    fun getTimeline(runId: String): RunTimeline {
        val entries = timelines[runId] ?: emptyList()
        return RunTimeline(runId, entries.toList())
    }

    /**
     * Lists all projected run summaries, optionally filtered by tenant.
     *
     * @param tenantId When non-null, only runs belonging to this tenant are returned.
     *   When `null`, all runs are returned regardless of tenant.
     * @return A list of [RunSummary] objects matching the filter criteria.
     * @see RunSummary
     */
    fun listRuns(tenantId: String? = null): List<RunSummary> {
        return if (tenantId != null) {
            summaries.values.filter { it.tenantId == tenantId }
        } else {
            summaries.values.toList()
        }
    }

    /**
     * Returns all tool call records reported by runtimes across every step of a run.
     *
     * Tool calls are recorded as reported by the adapter — Pekora does not mediate
     * them. This method is the primary audit surface for inspecting what tools an
     * agent invoked during execution.
     *
     * @param runId The unique identifier of the run to query.
     * @return A map of step ID to the list of [ToolCallRecord] instances for that step.
     *   Steps that reported no tool calls are omitted. Returns an empty map if the
     *   run is unknown or no tool calls were recorded.
     */
    fun getToolCalls(runId: String): Map<String, List<ToolCallRecord>> {
        return toolCallsByStep[runId]?.toMap() ?: emptyMap()
    }

    /**
     * Returns all runs that are currently in an active (non-terminal) state.
     *
     * Active states include [RunState.EXECUTING], [RunState.WAITING_FOR_APPROVAL],
     * and [RunState.WAITING_FOR_EXTERNAL_EVENT].
     *
     * @return A list of [RunSummary] objects for all currently active runs.
     * @see RunSummary
     * @see RunState
     */
    fun getActiveRuns(): List<RunSummary> {
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
