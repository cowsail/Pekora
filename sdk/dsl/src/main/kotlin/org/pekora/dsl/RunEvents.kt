/**
 * Canonical run events for event sourcing.
 *
 * These events form the persistent journal for each [org.pekora.engine.RunEntity].
 * Every state change in a run is captured as an immutable event, enabling:
 *
 * - **Replayable history** — reconstruct any past run state
 * - **Audit trail** — full record of what happened and when
 * - **Deterministic recovery** — replay events after a crash to restore state
 * - **Projections** — build read models from event streams
 *
 * @see org.pekora.engine.RunState for how events are applied to produce state
 * @see org.pekora.projection.RunProjectionStore for read-model projections
 */
package org.pekora.dsl

import kotlinx.serialization.Serializable

/**
 * Base interface for all run events.
 *
 * Every event is tagged with the [runId] it belongs to and the [timestamp] at which it occurred.
 * The [org.pekora.engine.RunEntity] persists these events via Pekko Persistence and
 * replays them on recovery to rebuild [org.pekora.engine.RunState].
 *
 * @property runId The unique identifier of the run this event belongs to.
 * @property timestamp Unix epoch milliseconds when the event occurred.
 */
sealed interface RunEvent {
    val runId: String
    val timestamp: Long
}

/**
 * Emitted when a new run is created.
 *
 * This is always the first event in a run's journal. It captures the initial parameters
 * and transitions the run from [RunState.CREATED] to [RunState.LOADING_DEFINITION].
 *
 * @property templateId The workflow template this run is based on.
 * @property versionNumber The specific workflow version to execute.
 * @property inputs The input parameters provided when the run was created.
 * @property tenantId Tenant scope for multi-tenant deployments.
 * @property correlationId Correlation ID for distributed tracing.
 */
@Serializable
data class RunCreated(
    override val runId: String,
    val templateId: String,
    val versionNumber: Int,
    val inputs: Map<String, String>,
    val tenantId: String = "",
    val correlationId: String = "",
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when the workflow definition has been loaded from the registry.
 *
 * Transitions the run from [RunState.LOADING_DEFINITION] to [RunState.READY].
 *
 * @property definition The complete workflow definition to execute.
 */
@Serializable
data class WorkflowLoaded(
    override val runId: String,
    val definition: WorkflowDefinition,
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when execution begins.
 *
 * Transitions the run from [RunState.READY] to [RunState.EXECUTING] and triggers
 * scheduling of the first step.
 */
@Serializable
data class RunStarted(
    override val runId: String,
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a step has been scheduled for execution but has not yet started.
 *
 * Sets the step state to [StepState.PENDING].
 *
 * @property stepId The step that was scheduled.
 */
@Serializable
data class StepScheduled(
    override val runId: String,
    val stepId: String,
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a step begins executing.
 *
 * Sets the step state to [StepState.RUNNING] and increments the attempt counter.
 *
 * @property stepId The step that started executing.
 * @property backend The adapter backend handling this execution.
 */
@Serializable
data class StepStarted(
    override val runId: String,
    val stepId: String,
    val backend: String = "",
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a step completes successfully.
 *
 * Sets the step state to [StepState.SUCCEEDED] and stores the output for
 * downstream steps to reference via `${'$'}{steps.stepId.output}` expressions.
 *
 * When the step was an agent execution (e.g. LangGraph), [toolCalls] captures
 * whatever tool invocations the runtime reported back. Pekora does not mediate
 * these calls — the runtime remains a black box — but the records are persisted
 * here so they are available for audit queries.
 *
 * @property stepId The step that completed.
 * @property output Key-value output data produced by the step.
 * @property toolCalls Tool calls reported by the runtime during this step, if any.
 * @property metrics Execution performance metrics.
 */
@Serializable
data class StepCompleted(
    override val runId: String,
    val stepId: String,
    val output: Map<String, String> = emptyMap(),
    val toolCalls: List<ToolCallRecord> = emptyList(),
    val metrics: ExecutionMetrics = ExecutionMetrics(),
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a step fails.
 *
 * If [retryable] is `true` and retries remain, a [StepRetryScheduled] event will follow.
 * Otherwise, the run may transition to [RunState.FAILED].
 *
 * @property stepId The step that failed.
 * @property error Human-readable error description.
 * @property retryable Whether this failure is eligible for retry.
 */
@Serializable
data class StepFailed(
    override val runId: String,
    val stepId: String,
    val error: String,
    val retryable: Boolean = true,
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a failed step is scheduled for retry.
 *
 * Sets the step state to [StepState.RETRYING].
 *
 * @property stepId The step being retried.
 * @property attempt The upcoming attempt number (2 = first retry, 3 = second retry, etc.).
 * @property nextRetryAt Unix epoch milliseconds when the retry will be attempted.
 */
@Serializable
data class StepRetryScheduled(
    override val runId: String,
    val stepId: String,
    val attempt: Int,
    val nextRetryAt: Long,
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a step requires human approval before the workflow can continue.
 *
 * Transitions the run to [RunState.WAITING_FOR_APPROVAL] and the step to [StepState.BLOCKED].
 * The approval request is forwarded to the [org.pekora.engine.ApprovalManager].
 *
 * @property stepId The approval step that is blocking.
 * @property approvalId Unique identifier for this approval request.
 * @property approvers List of authorized approver identifiers.
 */
@Serializable
data class ApprovalRequested(
    override val runId: String,
    val stepId: String,
    val approvalId: String,
    val approvers: List<String> = emptyList(),
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when an approval decision is received.
 *
 * If [approved] is `true`, the workflow advances to the next step.
 * If `false`, the step is set to [StepState.CANCELLED].
 *
 * @property stepId The approval step that was resolved.
 * @property approvalId The approval request identifier.
 * @property approved Whether the approval was granted.
 * @property approver The identity of the person who made the decision.
 * @property reason Optional reason provided with the decision.
 */
@Serializable
data class ApprovalReceived(
    override val runId: String,
    val stepId: String,
    val approvalId: String,
    val approved: Boolean,
    val approver: String = "",
    val reason: String = "",
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a run is paused (e.g., waiting for an external event).
 *
 * @property reason Human-readable reason for the pause.
 */
@Serializable
data class RunPaused(
    override val runId: String,
    val reason: String = "",
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a paused run is resumed.
 *
 * Transitions the run back to [RunState.EXECUTING] and re-schedules the next step.
 */
@Serializable
data class RunResumed(
    override val runId: String,
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a run completes successfully.
 *
 * This is the terminal success event. The run transitions to [RunState.COMPLETED].
 *
 * @property output Final output data from the workflow's result step.
 */
@Serializable
data class RunCompleted(
    override val runId: String,
    val output: Map<String, String> = emptyMap(),
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a run fails due to an unrecoverable error.
 *
 * This is the terminal failure event. The run transitions to [RunState.FAILED].
 *
 * @property error Human-readable description of the failure.
 */
@Serializable
data class RunFailed(
    override val runId: String,
    val error: String,
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent

/**
 * Emitted when a run is cancelled via the API.
 *
 * This is the terminal cancellation event. The run transitions to [RunState.CANCELLED].
 *
 * @property reason Optional reason for the cancellation.
 */
@Serializable
data class RunCancelled(
    override val runId: String,
    val reason: String = "",
    override val timestamp: Long = System.currentTimeMillis(),
) : RunEvent
