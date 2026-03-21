/**
 * Adapter interfaces for the Pekko Agent Workflow Framework.
 *
 * The adapter layer makes external runtimes pluggable. Each adapter implements a standard
 * contract so the framework can dispatch to any backend without knowing its internal details.
 *
 * The framework provides one primary adapter interface:
 * - [AgentRuntimeAdapter] — for executing steps in external agent runtimes (LangGraph, Strands, OpenClaw, etc.)
 *
 * And one infrastructure adapter:
 * - [WorkspaceAdapter] — for managing workspace lifecycle (repo checkout, sandbox environments)
 *
 * ## Implementing a Custom Adapter
 *
 * To add a new backend, implement [AgentRuntimeAdapter] and register it with the
 * [org.pekora.engine.StepExecutor]:
 *
 * ```kotlin
 * class MyCustomAdapter : AgentRuntimeAdapter {
 *     override val backendId = "my-custom-backend"
 *
 *     override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
 *         // Execute the step and return a normalized result
 *     }
 * }
 * ```
 *
 * @see org.pekora.adapters.langgraph.LangGraphAdapter
 * @see org.pekora.adapters.a2a.A2AAdapter
 * @see org.pekora.adapters.generic.GenericAdapter
 */
package org.pekora.adapters

import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Health status of an adapter backend.
 */
enum class HealthStatus { HEALTHY, UNHEALTHY, UNKNOWN }

/**
 * Health report returned by [AgentRuntimeAdapter.healthCheck].
 *
 * @property backendId The adapter's [AgentRuntimeAdapter.backendId].
 * @property status Overall health status.
 * @property message Optional detail message (e.g., error description).
 * @property latencyMs Round-trip latency in milliseconds, or 0 if unknown.
 */
data class AdapterHealth(
    val backendId: String,
    val status: HealthStatus,
    val message: String = "",
    val latencyMs: Long = 0,
)

/**
 * Adapter for executing workflow steps in an external agent runtime.
 *
 * This is the primary adapter interface for backends like LangGraph, Strands, and LangChain.
 * The framework delegates step execution to the adapter and expects a normalized
 * [StepExecutionResult] in return. The framework retains ownership of:
 * - Global workflow state
 * - Top-level retry semantics
 * - Approval lifecycle
 * - Workflow version pinning
 * - The canonical event timeline
 *
 * @see org.pekora.engine.StepExecutor for how adapters are dispatched
 */
interface AgentRuntimeAdapter {
    /**
     * Unique identifier for this backend (e.g., `"langgraph"`, `"strands"`, `"native"`).
     * Steps reference this via [AgentDefinition.backend].
     */
    val backendId: String

    /**
     * Executes a workflow step asynchronously.
     *
     * The adapter must:
     * 1. Map the framework [StepExecutionRequest] to the backend's native input format
     * 2. Execute the step in the external runtime
     * 3. Normalize the result into a [StepExecutionResult]
     * 4. Classify any failures (transient vs. permanent)
     *
     * @param request The canonical step execution request with resolved inputs and constraints.
     * @return A [CompletionStage] that completes with the normalized execution result.
     */
    fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult>

    /**
     * Cancels a running step execution.
     *
     * Default implementation is a no-op. Override if the backend supports cancellation.
     *
     * @param executionId The execution identifier to cancel.
     * @return A [CompletionStage] that completes when cancellation is acknowledged.
     */
    fun cancelStep(executionId: String): CompletionStage<Unit> {
        return CompletableFuture.completedFuture(Unit)
    }

    /**
     * Validates that a definition is compatible with this backend.
     *
     * Default implementation always returns valid. Override to add backend-specific
     * validation (e.g., verify a graph name exists in LangGraph).
     *
     * @param definition Backend-specific definition metadata to validate.
     * @return A [CompletionStage] with the validation result.
     */
    fun validateDefinition(definition: Map<String, String>): CompletionStage<ValidationResult> {
        return CompletableFuture.completedFuture(ValidationResult(valid = true))
    }

    /**
     * Performs a lightweight health check against the backend.
     *
     * Default implementation returns [HealthStatus.UNKNOWN]. Override to probe the real
     * backend endpoint and return [HealthStatus.HEALTHY] or [HealthStatus.UNHEALTHY].
     *
     * @return A [CompletionStage] with the current [AdapterHealth].
     */
    fun healthCheck(): CompletionStage<AdapterHealth> {
        return CompletableFuture.completedFuture(AdapterHealth(backendId, HealthStatus.UNKNOWN))
    }
}

/**
 * Adapter for managing workspace lifecycle.
 *
 * Workspaces provide isolated execution environments for skills that need file system access
 * (e.g., repo checkout, sandbox prep for coding loops, document generation).
 *
 * This is a Phase 2/3 adapter — not required for v1.
 */
interface WorkspaceAdapter {
    /**
     * Prepares a workspace for a step execution.
     *
     * @param request Configuration for the workspace (type, repo, branch, etc.).
     * @return A [CompletionStage] with a handle to the prepared workspace.
     */
    fun prepare(request: WorkspaceRequest): CompletionStage<WorkspaceHandle>

    /**
     * Cleans up a workspace after execution completes.
     *
     * @param handleId The handle ID of the workspace to clean up.
     * @return A [CompletionStage] that completes when cleanup is done.
     */
    fun cleanup(handleId: String): CompletionStage<Unit>
}

/**
 * Request to prepare a workspace.
 *
 * @property runId The parent run ID for correlation.
 * @property stepId The step that needs the workspace.
 * @property workspaceType Type of workspace (e.g., `"git-checkout"`, `"sandbox"`).
 * @property config Workspace-specific configuration (e.g., repo URL, branch).
 */
data class WorkspaceRequest(
    val runId: String,
    val stepId: String,
    val workspaceType: String,
    val config: Map<String, String> = emptyMap(),
)

/**
 * Handle to a prepared workspace.
 *
 * @property handleId Unique identifier for this workspace instance.
 * @property workspacePath File system path to the workspace root.
 * @property metadata Additional metadata about the workspace.
 */
data class WorkspaceHandle(
    val handleId: String,
    val workspacePath: String,
    val metadata: Map<String, String> = emptyMap(),
)

/**
 * Adapter that can release resources associated with a completed workflow run.
 *
 * Implement this interface alongside [AgentRuntimeAdapter] when an adapter maintains
 * per-run state (e.g., PER_RUN Pekko actor instances) that should be released when the
 * run reaches a terminal state. The framework will call [cleanupRun] automatically.
 */
interface CleanableAdapter {
    /**
     * Releases all per-run resources associated with [runId].
     *
     * Called by the framework when a run completes, fails, or is cancelled.
     * Implementations must be idempotent and safe to call on unknown run IDs.
     *
     * @param runId The run whose resources should be released.
     */
    fun cleanupRun(runId: String)
}

/**
 * Result of validating a definition against an adapter.
 *
 * @property valid Whether the definition is compatible with the adapter.
 * @property errors List of validation error messages (empty if valid).
 */
data class ValidationResult(
    val valid: Boolean,
    val errors: List<String> = emptyList(),
)
