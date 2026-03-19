/**
 * # RunState — Persistent State for Event-Sourced RunEntity
 *
 * This file defines [RunState], the data class that represents the in-memory state of a
 * [RunEntity] actor. State is built exclusively by applying [RunEvent] instances through
 * the [RunState.applyEvent] method, following the event sourcing pattern.
 *
 * ## Mutable Maps Design
 *
 * Several fields in [RunState] use [MutableMap] and [MutableList] rather than immutable
 * collections. This is a deliberate design choice to allow efficient in-place updates
 * during event application (e.g., updating a single step's state without copying the
 * entire map). The `applyEvent` method creates a shallow copy of the `RunState` via
 * `this.copy()`, then mutates the maps on the copy before returning it. This avoids
 * allocating new map instances for every event while still preserving the previous state
 * reference for Pekko Persistence's internal bookkeeping.
 *
 * ## Recovery
 *
 * During actor recovery, Pekko Persistence replays all persisted events starting from
 * [RunState.empty] (or the latest snapshot). Each event is applied in order via
 * [applyEvent], reconstructing the full in-memory state.
 *
 * @see RunEntity
 * @see RunEvent
 */
package org.pekora.engine

import org.pekora.dsl.*
import org.pekora.dsl.RunState as RunStatus

/**
 * The persistent state of a [RunEntity], updated exclusively by applying [RunEvent] instances.
 *
 * This data class captures the entire in-memory state of a single workflow run, including
 * metadata, the loaded workflow definition, step execution progress, outputs, and pending
 * approvals. It is serialized for snapshotting and reconstructed during recovery by replaying
 * events from the journal.
 *
 * @property runId The unique identifier of the workflow run.
 * @property status The current lifecycle status of the run (CREATED, READY, EXECUTING, COMPLETED, etc.).
 * @property templateId The identifier of the workflow template that was instantiated.
 * @property versionNumber The version number of the workflow template.
 * @property definition The parsed [WorkflowDefinition], or `null` if not yet loaded.
 * @property inputs The initial input parameters provided when the run was created.
 * @property outputs A mutable map of aggregated outputs from completed steps. Mutable for
 *                   efficient in-place updates during event application.
 * @property stepStates A mutable map tracking the current [StepState] of each step by step ID.
 *                      Updated as steps transition through PENDING, RUNNING, SUCCEEDED, FAILED, etc.
 * @property stepOutputs A mutable map of step outputs keyed by step ID. Each value is itself a
 *                       map of output key-value pairs produced by the step's execution.
 * @property stepToolCalls A mutable map of tool call records keyed by step ID. Populated from
 *                         [StepCompleted] events when the runtime reports tool invocations.
 *                         Pekora does not mediate these calls; they are stored for audit only.
 * @property stepAttempts A mutable map tracking the current attempt number for each step, used
 *                        by the retry logic to determine whether further retries are allowed.
 * @property pendingApprovals A mutable map of pending approval requests, keyed by approval ID,
 *                            with values being the corresponding step ID. Entries are added when
 *                            an [ApprovalRequested] event is applied and removed upon [ApprovalReceived].
 * @property tenantId The tenant identifier for multi-tenant deployments.
 * @property correlationId The correlation identifier for distributed tracing.
 * @property error The error message if the run has failed; `null` otherwise.
 * @property events A mutable list of all [RunEvent] instances applied to this state, providing
 *                  a complete audit trail of the run's history.
 *
 * @see RunEntity
 * @see RunEvent
 */
data class RunState(
    val runId: String,
    val status: RunStatus,
    val templateId: String = "",
    val versionNumber: Int = 0,
    val definition: WorkflowDefinition? = null,
    val inputs: Map<String, String> = emptyMap(),
    val outputs: MutableMap<String, String> = mutableMapOf(),
    val stepStates: MutableMap<String, StepState> = mutableMapOf(),
    val stepOutputs: MutableMap<String, Map<String, String>> = mutableMapOf(),
    val stepToolCalls: MutableMap<String, List<ToolCallRecord>> = mutableMapOf(),
    val stepAttempts: MutableMap<String, Int> = mutableMapOf(),
    val pendingApprovals: MutableMap<String, String> = mutableMapOf(), // approvalId -> stepId
    val tenantId: String = "",
    val correlationId: String = "",
    val error: String? = null,
    val events: MutableList<RunEvent> = mutableListOf(),
) {
    /**
     * Companion object providing factory methods for [RunState].
     */
    companion object {
        /**
         * Creates an empty initial state for a new or recovering [RunEntity].
         *
         * The returned state has status [CREATED][RunStatus.CREATED], no loaded definition,
         * and empty maps for all step tracking fields.
         *
         * @param runId The unique identifier of the workflow run.
         * @return A fresh [RunState] ready to accept a [RunCreated] event.
         */
        fun empty(runId: String) = RunState(runId = runId, status = RunStatus.CREATED)
    }

    /**
     * Applies a single [RunEvent] to this state, producing a new [RunState] that reflects
     * the event's effect.
     *
     * This method is the core of the event sourcing model. It is called both during normal
     * operation (after an event is persisted to the journal) and during recovery (when events
     * are replayed). The method creates a shallow copy of the current state, appends the event
     * to the audit trail, and then applies event-specific mutations:
     *
     * - **[RunCreated]**: Sets template metadata, inputs, and transitions to `LOADING_DEFINITION`.
     * - **[WorkflowLoaded]**: Attaches the workflow definition and transitions to `READY`.
     * - **[RunStarted]**: Transitions to `EXECUTING`.
     * - **[StepScheduled]**: Marks the step as `PENDING`.
     * - **[StepStarted]**: Marks the step as `RUNNING` and increments the attempt counter.
     * - **[StepCompleted]**: Marks the step as `SUCCEEDED` and records its output.
     * - **[StepFailed]**: Marks the step as `FAILED`.
     * - **[StepRetryScheduled]**: Marks the step as `RETRYING` and updates the attempt counter.
     * - **[ApprovalRequested]**: Marks the step as `BLOCKED`, records the pending approval, and
     *   transitions to `WAITING_FOR_APPROVAL`.
     * - **[ApprovalReceived]**: Removes the pending approval and marks the step as `SUCCEEDED`
     *   (if approved) or `CANCELLED` (if rejected), transitioning back to `EXECUTING`.
     * - **[RunPaused]**: Transitions to `WAITING_FOR_EXTERNAL_EVENT`.
     * - **[RunResumed]**: Transitions back to `EXECUTING`.
     * - **[RunCompleted]**: Records final outputs and transitions to `COMPLETED`.
     * - **[RunFailed]**: Records the error message and transitions to `FAILED`.
     * - **[RunCancelled]**: Transitions to `CANCELLED`.
     *
     * @param event The [RunEvent] to apply to the current state.
     * @return A new [RunState] reflecting the applied event.
     *
     * @see RunEvent
     * @see RunEntity.eventHandler
     */
    fun applyEvent(event: RunEvent): RunState {
        val newState = this.copy()
        newState.events.add(event)

        return when (event) {
            is RunCreated -> newState.copy(
                templateId = event.templateId,
                versionNumber = event.versionNumber,
                inputs = event.inputs,
                tenantId = event.tenantId,
                correlationId = event.correlationId,
                status = RunStatus.LOADING_DEFINITION,
            )
            is WorkflowLoaded -> newState.copy(
                definition = event.definition,
                status = RunStatus.READY,
            )
            is RunStarted -> newState.copy(status = RunStatus.EXECUTING)
            is StepScheduled -> {
                newState.stepStates[event.stepId] = StepState.PENDING
                newState
            }
            is StepStarted -> {
                newState.stepStates[event.stepId] = StepState.RUNNING
                newState.stepAttempts[event.stepId] = (newState.stepAttempts[event.stepId] ?: 0) + 1
                newState
            }
            is StepCompleted -> {
                newState.stepStates[event.stepId] = StepState.SUCCEEDED
                newState.stepOutputs[event.stepId] = event.output
                if (event.toolCalls.isNotEmpty()) {
                    newState.stepToolCalls[event.stepId] = event.toolCalls
                }
                newState
            }
            is StepFailed -> {
                newState.stepStates[event.stepId] = StepState.FAILED
                newState
            }
            is StepRetryScheduled -> {
                newState.stepStates[event.stepId] = StepState.RETRYING
                newState.stepAttempts[event.stepId] = event.attempt
                newState
            }
            is ApprovalRequested -> {
                newState.stepStates[event.stepId] = StepState.BLOCKED
                newState.pendingApprovals[event.approvalId] = event.stepId
                newState.copy(status = RunStatus.WAITING_FOR_APPROVAL)
            }
            is ApprovalReceived -> {
                newState.pendingApprovals.remove(event.approvalId)
                if (event.approved) {
                    newState.stepStates[event.stepId] = StepState.SUCCEEDED
                    newState.copy(status = RunStatus.EXECUTING)
                } else {
                    newState.stepStates[event.stepId] = StepState.CANCELLED
                    newState.copy(status = RunStatus.EXECUTING)
                }
            }
            is RunPaused -> newState.copy(status = RunStatus.WAITING_FOR_EXTERNAL_EVENT)
            is RunResumed -> newState.copy(status = RunStatus.EXECUTING)
            is RunCompleted -> newState.copy(
                status = RunStatus.COMPLETED,
                outputs = event.output.toMutableMap(),
            )
            is RunFailed -> newState.copy(
                status = RunStatus.FAILED,
                error = event.error,
            )
            is RunCancelled -> newState.copy(status = RunStatus.CANCELLED)
        }
    }
}
