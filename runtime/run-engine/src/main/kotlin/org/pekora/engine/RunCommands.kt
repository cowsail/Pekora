/**
 * # RunCommands — Actor Protocol for RunEntity
 *
 * This file defines the complete set of commands (messages) and responses that form the
 * typed actor protocol for [RunEntity]. In Apache Pekko Typed, an actor's public API is
 * defined entirely by the messages it accepts. The sealed interface [RunCommand] ensures
 * that the compiler can verify exhaustive handling of all command types.
 *
 * ## Commands
 *
 * Commands are sent to a [RunEntity] actor reference (obtained via Cluster Sharding) to
 * drive the workflow run lifecycle. Most commands carry a `replyTo` actor reference so
 * the sender can receive an acknowledgement or query result.
 *
 * ## Responses
 *
 * [RunCommandResponse] is the standard acknowledgement for mutating commands, indicating
 * success or failure with an optional message. [RunStatusResponse] is the richer response
 * returned by the [GetRunStatus] query command.
 *
 * @see RunEntity
 * @see RunEvent
 * @see RunState
 */
package org.pekora.engine

import org.apache.pekko.actor.typed.ActorRef
import org.pekora.dsl.*

/**
 * Sealed interface representing all commands accepted by [RunEntity].
 *
 * Every message sent to a `RunEntity` actor must implement this interface. The sealed
 * nature allows the Kotlin compiler to enforce exhaustive pattern matching when handling
 * commands, and limits the actor's public API to a well-defined set of operations.
 *
 * @see RunEntity
 */
sealed interface RunCommand

/**
 * Command to initialize a new workflow run with the given metadata.
 *
 * This is typically the first command sent to a newly sharded [RunEntity]. It records the
 * template reference, version, initial inputs, and optional tenant/correlation identifiers.
 * The entity will reject this command if the run has already been created.
 *
 * @property templateId The identifier of the workflow template to instantiate.
 * @property versionNumber The version number of the workflow template.
 * @property inputs The initial input parameters provided by the caller, keyed by parameter name.
 * @property tenantId Optional tenant identifier for multi-tenant deployments. Defaults to empty string.
 * @property correlationId Optional correlation identifier for distributed tracing. Defaults to empty string.
 * @property replyTo The actor reference to receive the [RunCommandResponse] acknowledgement.
 *
 * @see RunEntity
 */
data class CreateRun(
    val templateId: String,
    val versionNumber: Int,
    val inputs: Map<String, String>,
    val tenantId: String = "",
    val correlationId: String = "",
    val replyTo: ActorRef<RunCommandResponse>,
) : RunCommand

/**
 * Command to attach a parsed [WorkflowDefinition] to the run.
 *
 * Sent after [CreateRun] once the workflow definition has been resolved and parsed from
 * the template registry. This transitions the run from `LOADING_DEFINITION` to `READY`,
 * enabling it to accept [StartRun].
 *
 * @property definition The fully parsed workflow definition containing steps, agents, and configuration.
 * @property replyTo The actor reference to receive the [RunCommandResponse] acknowledgement.
 *
 * @see WorkflowDefinition
 */
data class LoadWorkflow(
    val definition: WorkflowDefinition,
    val replyTo: ActorRef<RunCommandResponse>,
) : RunCommand

/**
 * Command to begin executing the workflow.
 *
 * The run must be in the `READY` state (i.e., a workflow definition must have been loaded).
 * Upon acceptance, the entity transitions to `EXECUTING` and schedules the first step
 * for execution via the [StepExecutor].
 *
 * @property replyTo The actor reference to receive the [RunCommandResponse] acknowledgement.
 *
 * @see StepExecutor
 */
data class StartRun(
    val replyTo: ActorRef<RunCommandResponse>,
) : RunCommand

/**
 * Command delivering the result of a step execution back to the [RunEntity].
 *
 * Sent by the [StepExecutor] (via [StepResultInternal]) after a step completes, whether
 * successfully or with failure. The entity uses this to advance the workflow, schedule
 * retries, or fail the run depending on the result status and retry configuration.
 *
 * @property stepId The identifier of the step whose execution has completed.
 * @property result The [StepExecutionResult] containing status, output, error, and metrics.
 *
 * @see StepExecutor
 * @see StepExecutionResult
 */
data class StepResult(
    val stepId: String,
    val result: StepExecutionResult,
) : RunCommand

/**
 * Command delivering an approval decision for a step that required human approval.
 *
 * Sent by the [ApprovalManager] when an approver grants or denies a pending approval
 * request. If approved, the workflow advances past the approval gate; if rejected, the
 * step is marked as cancelled but the run continues executing.
 *
 * @property stepId The identifier of the approval step in the workflow definition.
 * @property approvalId The unique identifier of the approval request being responded to.
 * @property approved Whether the approval was granted (`true`) or rejected (`false`).
 * @property approver The identity of the person or system that made the approval decision. Defaults to empty string.
 * @property reason An optional explanation for the approval or rejection. Defaults to empty string.
 *
 * @see ApprovalManager
 */
data class ApprovalResponse(
    val stepId: String,
    val approvalId: String,
    val approved: Boolean,
    val approver: String = "",
    val reason: String = "",
) : RunCommand

/**
 * Command to cancel a running workflow.
 *
 * Can be sent at any time to abort the run. The entity persists a [RunCancelled] event
 * and transitions to the `CANCELLED` state.
 *
 * @property reason An optional human-readable reason for the cancellation. Defaults to empty string.
 * @property replyTo The actor reference to receive the [RunCommandResponse] acknowledgement.
 */
data class CancelRun(
    val reason: String = "",
    val replyTo: ActorRef<RunCommandResponse>,
) : RunCommand

/**
 * Command to resume a paused or externally-blocked workflow run.
 *
 * Transitions the run back to `EXECUTING` and re-schedules the next pending step.
 *
 * @property replyTo The actor reference to receive the [RunCommandResponse] acknowledgement.
 */
data class ResumeRun(
    val replyTo: ActorRef<RunCommandResponse>,
) : RunCommand

/**
 * Query command to retrieve the current status of the workflow run.
 *
 * This is a read-only command that does not persist any events. The entity replies
 * immediately with a [RunStatusResponse] containing the current state snapshot.
 *
 * @property replyTo The actor reference to receive the [RunStatusResponse] with the current run status.
 *
 * @see RunStatusResponse
 */
data class GetRunStatus(
    val replyTo: ActorRef<RunStatusResponse>,
) : RunCommand

// --- Responses ---

/**
 * Standard acknowledgement response for mutating [RunCommand] operations.
 *
 * Returned by [CreateRun], [LoadWorkflow], [StartRun], [CancelRun], and [ResumeRun]
 * to indicate whether the command was accepted and processed.
 *
 * @property success `true` if the command was accepted and the corresponding event was persisted;
 *                   `false` if the command was rejected due to invalid state or precondition failure.
 * @property message A human-readable description of the outcome, useful for logging and diagnostics.
 *                   Defaults to empty string.
 */
data class RunCommandResponse(
    val success: Boolean,
    val message: String = "",
)

/**
 * Response to the [GetRunStatus] query command, providing a snapshot of the current run state.
 *
 * @property runId The unique identifier of the workflow run.
 * @property status The current lifecycle status of the run (e.g., EXECUTING, COMPLETED, FAILED).
 * @property stepStates A map of step identifiers to their current [StepState] (PENDING, RUNNING, SUCCEEDED, etc.).
 * @property outputs The aggregated output map from completed steps.
 * @property stepToolCalls Tool calls reported by the runtime for each step, keyed by step ID.
 *   Populated only for steps where the runtime included tool call records in its response.
 *   Pekora does not mediate these calls — they are surfaced here for audit purposes only.
 * @property error The error message if the run is in a FAILED state; `null` otherwise.
 *
 * @see GetRunStatus
 * @see RunState
 */
data class RunStatusResponse(
    val runId: String,
    val status: org.pekora.dsl.RunState,
    val stepStates: Map<String, StepState>,
    val outputs: Map<String, String>,
    val stepToolCalls: Map<String, List<ToolCallRecord>> = emptyMap(),
    val parallelGroups: Map<String, ParallelGroupState> = emptyMap(),
    val subworkflowChildren: Map<String, SubworkflowChildState> = emptyMap(),
    val error: String? = null,
)
