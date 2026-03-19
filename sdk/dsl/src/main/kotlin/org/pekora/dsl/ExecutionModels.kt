/**
 * Execution models for the step execution pipeline.
 *
 * This file defines the canonical request/result envelopes used for all step
 * executions, as well as the run and step state enumerations.
 *
 * Every step execution flows through these canonical types, ensuring a uniform
 * interface between the [org.pekora.engine.RunEntity],
 * [org.pekora.engine.StepExecutor], and adapter implementations.
 *
 * @see org.pekora.adapters.AgentRuntimeAdapter
 */
package org.pekora.dsl

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

// --- Step Execution Request/Result (Section 8) ---

/**
 * Canonical request envelope for executing a workflow step.
 *
 * The [org.pekora.engine.StepExecutor] constructs this from the [StepDefinition]
 * and dispatches it to the appropriate adapter based on [backend].
 *
 * @property runId The unique identifier of the parent run.
 * @property stepId The unique identifier of the step within the workflow.
 * @property stepKind The type of step being executed.
 * @property backend The adapter backend to use for execution (e.g., `"langgraph"`, `"openclaw-tools"`).
 * @property definitionRef Reference to the external definition (e.g., agent ID, tool ID, graph name).
 * @property input Resolved input parameters for the step.
 * @property context Additional contextual data passed to the adapter.
 * @property constraints Execution constraints (timeout, allowed tools, budget).
 * @property correlationId Correlation ID for distributed tracing and log aggregation.
 */
@Serializable
data class StepExecutionRequest(
    @SerialName("run_id") val runId: String,
    @SerialName("step_id") val stepId: String,
    @SerialName("step_kind") val stepKind: StepKind,
    val backend: String,
    @SerialName("definition_ref") val definitionRef: String = "",
    val input: Map<String, String> = emptyMap(),
    val context: Map<String, String> = emptyMap(),
    val constraints: StepConstraints = StepConstraints(),
    @SerialName("correlation_id") val correlationId: String = "",
)

/**
 * Execution constraints applied to a step.
 *
 * These are derived from the step definition and active policies.
 * Adapters must respect these constraints or report a violation.
 *
 * @property timeoutSeconds Maximum allowed execution time in seconds.
 * @property budget Token budget constraints.
 */
@Serializable
data class StepConstraints(
    @SerialName("timeout_seconds") val timeoutSeconds: Int = 300,
    val budget: BudgetConstraints = BudgetConstraints(),
)

/**
 * Token budget constraints for a step execution.
 *
 * @property maxTokens Maximum number of tokens the step may consume across all LLM calls.
 */
@Serializable
data class BudgetConstraints(
    @SerialName("max_tokens") val maxTokens: Long = 50000,
)

/**
 * Canonical result envelope returned after a step execution completes.
 *
 * Every adapter must normalize its output into this format. The [org.pekora.engine.RunEntity]
 * uses this to update run state, persist events, and decide whether to advance, retry, or fail.
 *
 * @property status The outcome of the execution.
 * @property output Key-value output data produced by the step.
 * @property events List of events that occurred during execution (for timeline/audit).
 * @property artifacts List of artifact identifiers produced (e.g., file paths, URLs).
 * @property toolCalls Record of tool calls made during execution.
 * @property metrics Execution performance metrics.
 * @property error Error message if the step failed, `null` on success.
 */
@Serializable
data class StepExecutionResult(
    val status: StepResultStatus,
    val output: Map<String, String> = emptyMap(),
    val events: List<StepEvent> = emptyList(),
    val artifacts: List<String> = emptyList(),
    @SerialName("tool_calls") val toolCalls: List<ToolCallRecord> = emptyList(),
    val metrics: ExecutionMetrics = ExecutionMetrics(),
    val error: String? = null,
)

/**
 * Outcome status of a step execution.
 */
@Serializable
enum class StepResultStatus {
    /** Step completed successfully and produced valid output. */
    @SerialName("succeeded") SUCCEEDED,
    /** Step failed due to an error (may be retryable). */
    @SerialName("failed") FAILED,
    /** Step was cancelled before completion. */
    @SerialName("cancelled") CANCELLED,
    /** Step exceeded its timeout constraint. */
    @SerialName("timed_out") TIMED_OUT,
}

/**
 * An event that occurred during step execution.
 *
 * Adapters emit these to provide fine-grained observability into what happened
 * during a step (e.g., intermediate progress, tool invocations).
 *
 * @property type Event type identifier (e.g., `"ToolInvocationCompleted"`, `"StepProgressed"`).
 * @property timestamp ISO-8601 timestamp of when the event occurred.
 * @property data Additional event-specific data.
 */
@Serializable
data class StepEvent(
    val type: String,
    val timestamp: String = "",
    val data: Map<String, String> = emptyMap(),
)

/**
 * Record of a tool call made during step execution.
 *
 * Captured for audit trail and cost tracking purposes.
 *
 * @property tool The tool identifier that was called.
 * @property input Input parameters passed to the tool.
 * @property output Output data returned by the tool.
 * @property durationMs How long the tool call took in milliseconds.
 */
@Serializable
data class ToolCallRecord(
    val tool: String,
    val input: Map<String, String> = emptyMap(),
    val output: Map<String, String> = emptyMap(),
    @SerialName("duration_ms") val durationMs: Long = 0,
)

/**
 * Performance metrics for a step execution.
 *
 * @property durationMs Total execution duration in milliseconds.
 * @property tokensUsed Total tokens consumed across all LLM calls.
 */
@Serializable
data class ExecutionMetrics(
    @SerialName("duration_ms") val durationMs: Long = 0,
    @SerialName("tokens_used") val tokensUsed: Long = 0,
)

// --- Run States ---

/**
 * The lifecycle states of a workflow run.
 *
 * A run progresses through these states as managed by the [org.pekora.engine.RunEntity]:
 *
 * ```
 * CREATED -> LOADING_DEFINITION -> READY -> EXECUTING -> COMPLETED
 *                                              |
 *                                              +-> WAITING_FOR_APPROVAL
 *                                              +-> WAITING_FOR_EXTERNAL_EVENT
 *                                              +-> FAILED
 *                                              +-> CANCELLED
 * ```
 */
@Serializable
enum class RunState {
    /** Run has been created but the workflow definition has not been loaded yet. */
    CREATED,
    /** The workflow definition is being loaded from the registry. */
    LOADING_DEFINITION,
    /** The workflow definition is loaded and the run is ready to start. */
    READY,
    /** The run is actively executing steps. */
    EXECUTING,
    /** The run is paused at an approval checkpoint awaiting human decision. */
    WAITING_FOR_APPROVAL,
    /** The run is paused waiting for an external event. */
    WAITING_FOR_EXTERNAL_EVENT,
    /** The run completed successfully with output. */
    COMPLETED,
    /** The run failed due to an unrecoverable error. */
    FAILED,
    /** The run was cancelled by an API request. */
    CANCELLED,
}

/**
 * The lifecycle states of an individual step within a run.
 */
@Serializable
enum class StepState {
    /** Step has not started yet. */
    PENDING,
    /** Step is currently executing. */
    RUNNING,
    /** Step completed successfully. */
    SUCCEEDED,
    /** Step failed and has exhausted retries. */
    FAILED,
    /** Step failed but a retry has been scheduled. */
    RETRYING,
    /** Step is blocked (e.g., waiting for approval). */
    BLOCKED,
    /** Step was skipped (e.g., branch not taken in a decision step). */
    SKIPPED,
    /** Step was cancelled. */
    CANCELLED,
}
