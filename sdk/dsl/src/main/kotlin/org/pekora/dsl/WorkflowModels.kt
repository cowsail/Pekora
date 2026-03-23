/**
 * Core domain models for the Pekko Agent Workflow Framework.
 *
 * This package defines the declarative workflow DSL as Kotlin data classes.
 * A [WorkflowDefinition] is the parsed, in-memory representation of a workflow YAML file.
 * Once published, a workflow becomes an immutable [WorkflowVersion] pinned to a [WorkflowTemplate].
 *
 * @see WorkflowParser for YAML-to-model parsing
 * @see org.pekora.engine.RunEntity for runtime execution of workflows
 */
package org.pekora.dsl

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

/**
 * Top-level workflow definition — the parsed representation of a workflow YAML/JSON.
 *
 * A workflow definition contains all the metadata, agent declarations,
 * policy bindings, and the step graph that describes how the workflow executes.
 *
 * Workflow definitions are immutable once published as a [WorkflowVersion].
 *
 * Example YAML that produces a [WorkflowDefinition]:
 * ```yaml
 * workflow:
 *   name: issue-to-pr
 *   version: 1
 *   steps:
 *     - id: plan
 *       type: agent
 *       agent: planner
 *       next: done
 *     - id: done
 *       type: result
 * ```
 *
 * @property name Human-readable name of the workflow.
 * @property version Monotonically increasing version number.
 * @property description Optional description of what the workflow does.
 * @property inputs Schema definition for the workflow's required inputs.
 * @property outputs Schema definition for the workflow's expected outputs.
 * @property agents List of agent declarations available to steps in this workflow.
 * @property policies List of policy references that govern execution.
 * @property steps The ordered list of steps that form the workflow graph.
 */
@Serializable
data class WorkflowDefinition(
    val name: String,
    val version: Int,
    val description: String = "",
    val inputs: SchemaDefinition? = null,
    val outputs: SchemaDefinition? = null,
    val agents: List<AgentDefinition> = emptyList(),
    val policies: List<PolicyReference> = emptyList(),
    val steps: List<StepDefinition>,
)

// --- Schema ---

/**
 * JSON Schema-like definition for workflow input/output contracts.
 *
 * Used to declare and validate the shape of data flowing into and out of workflows and steps.
 *
 * @property type The root type, typically `"object"`.
 * @property required List of field names that must be present.
 * @property properties Map of field names to their [PropertySchema] definitions.
 */
@Serializable
data class SchemaDefinition(
    val type: String = "object",
    val required: List<String> = emptyList(),
    val properties: Map<String, PropertySchema> = emptyMap(),
)

/**
 * Schema for a single property within a [SchemaDefinition].
 *
 * @property type The data type (e.g., `"string"`, `"integer"`, `"boolean"`).
 * @property description Human-readable description of this property.
 * @property default Optional default value as a string representation.
 */
@Serializable
data class PropertySchema(
    val type: String,
    val description: String = "",
    val default: String? = null,
)

// --- Agent ---

/**
 * Declares an agent available within a workflow.
 *
 * An agent represents an AI model execution context backed by a specific runtime.
 * Steps of type [StepKind.AGENT] reference an agent by its [id].
 *
 * @property id Unique identifier for this agent within the workflow.
 * @property backend The runtime backend that executes this agent (e.g., `"langgraph"`, `"openclaw-skill"`, `"native"`).
 * @property modelProfile Named model configuration profile (e.g., `"planning-default"`, `"coding-default"`).
 * @property description Human-readable description of the agent's purpose.
 * @property config Backend-specific configuration key-value pairs.
 */
@Serializable
data class AgentDefinition(
    val id: String,
    val backend: String,
    @SerialName("model_profile") val modelProfile: String = "default",
    val description: String = "",
    val config: Map<String, String> = emptyMap(),
)

/**
 * Reference to a policy that governs workflow or step execution.
 *
 * Policies can be referenced by [id] from a policy registry, or defined [inline]
 * directly within the workflow YAML.
 *
 * @property id Identifier of a registered policy, or empty if inline-only.
 * @property inline An optional inline [PolicyDefinition] embedded in the workflow.
 */
@Serializable
data class PolicyReference(
    val id: String,
    val inline: PolicyDefinition? = null,
)

// --- Steps ---

/**
 * Enumeration of all supported step types in the workflow DSL.
 *
 * Each step kind determines how the [org.pekora.engine.StepExecutor] dispatches execution:
 *
 * - [AGENT] — delegated to an [org.pekora.adapters.AgentRuntimeAdapter]
 * - [DECISION] — evaluates conditions and branches
 * - [APPROVAL] — pauses execution until a human approves or rejects
 * - [PARALLEL] — executes multiple steps concurrently (Phase 4)
 * - [SUBWORKFLOW] — invokes a nested workflow (Phase 4)
 * - [WAIT] — pauses for an external event
 * - [RESULT] — captures final output and completes the workflow
 */
@Serializable
enum class StepKind {
    @SerialName("agent") AGENT,
    @SerialName("decision") DECISION,
    @SerialName("approval") APPROVAL,
    @SerialName("parallel") PARALLEL,
    @SerialName("subworkflow") SUBWORKFLOW,
    @SerialName("wait") WAIT,
    @SerialName("result") RESULT,
}

/**
 * A single step in the workflow graph.
 *
 * Steps are the fundamental unit of execution. Each step has a [type] that determines
 * how it is executed, and a [next] pointer that links it to the following step in the graph.
 * Steps can reference prior step outputs using `${'$'}{steps.stepId.output}` expressions in their [input] maps.
 *
 * @property id Unique identifier for this step within the workflow.
 * @property type The kind of step, determining execution behavior.
 * @property agent Reference to an [AgentDefinition.id] (required when [type] is [StepKind.AGENT]).
 * @property input Map of input parameters. Values may contain `${'$'}{...}` expression references.
 * @property output Map of output parameters for [StepKind.RESULT] steps.
 * @property next The ID of the next step to execute after this one completes, or `null` if terminal.
 * @property branches List of conditional branches for [StepKind.DECISION] steps.
 * @property parallel List of step IDs to execute concurrently for [StepKind.PARALLEL] steps.
 * @property joinNext The step to advance to after all parallel branches complete.
 * @property subworkflow Reference to a workflow template ID for [StepKind.SUBWORKFLOW] steps.
 * @property approvers List of approver identifiers for [StepKind.APPROVAL] steps.
 * @property timeout Step-level timeout in seconds, overriding any policy default.
 * @property retries Retry configuration for transient failures.
 * @property policy Reference to a specific policy governing this step.
 * @property description Human-readable description of what this step does.
 */
@Serializable
data class StepDefinition(
    val id: String,
    val type: StepKind,
    val agent: String? = null,
    val input: Map<String, String> = emptyMap(),
    val output: Map<String, String> = emptyMap(),
    val next: String? = null,
    val branches: List<BranchDefinition> = emptyList(),
    val parallel: List<String> = emptyList(),
    @SerialName("join_next") val joinNext: String? = null,
    val subworkflow: String? = null,
    @SerialName("subworkflow_version") val subworkflowVersion: Int? = null,
    val approvers: List<String> = emptyList(),
    val timeout: Int? = null,
    val retries: RetryConfig? = null,
    val policy: String? = null,
    val description: String = "",
)

/**
 * A conditional branch in a [StepKind.DECISION] step.
 *
 * @property condition Expression to evaluate (e.g., `"classification == 'bug'"`).
 * @property next Step ID to advance to if the condition is true.
 */
@Serializable
data class BranchDefinition(
    val condition: String,
    val next: String,
)

/**
 * Retry configuration for a step.
 *
 * When a step fails with a retryable error, the framework will retry execution
 * up to [maxAttempts] times with exponential backoff.
 *
 * The delay before retry N is: `backoffMs * (multiplier ^ (N - 1))`
 *
 * @property maxAttempts Maximum number of attempts (including the initial attempt).
 * @property backoffMs Base backoff delay in milliseconds before the first retry.
 * @property multiplier Multiplier applied to the backoff for each subsequent retry.
 */
@Serializable
data class RetryConfig(
    @SerialName("max_attempts") val maxAttempts: Int = 3,
    @SerialName("backoff_ms") val backoffMs: Long = 1000,
    val multiplier: Double = 2.0,
)

// --- Policy ---

/**
 * Definition of a policy that governs step execution.
 *
 * Policies are evaluated by the [org.pekora.policy.PolicyGuard] before a step executes.
 * They control which backends, models, tools, and skills are permitted, set cost and timeout
 * limits, classify side effects, and gate execution on human approval.
 *
 * Multiple policies may apply to a single step. When they overlap, the most restrictive
 * constraint wins (e.g., the lowest timeout, the intersection of allowed tools).
 *
 * @property id Unique identifier for this policy.
 * @property allowedBackends List of permitted backend identifiers. Empty means all are allowed.
 * @property allowedModels List of permitted model identifiers. Empty means all are allowed.
 * @property maxTokens Maximum token budget for a single step execution.
 * @property timeoutSeconds Maximum execution time in seconds.
 * @property sideEffectClass Classification of the side effects this policy permits.
 * @property requireApproval Whether steps governed by this policy require human approval.
 *
 * @see SideEffectClass
 */
@Serializable
data class PolicyDefinition(
    val id: String = "",
    @SerialName("allowed_backends") val allowedBackends: List<String> = emptyList(),
    @SerialName("allowed_models") val allowedModels: List<String> = emptyList(),
    @SerialName("max_tokens") val maxTokens: Long? = null,
    @SerialName("timeout_seconds") val timeoutSeconds: Int? = null,
    @SerialName("side_effect_class") val sideEffectClass: SideEffectClass = SideEffectClass.READ_ONLY,
    @SerialName("require_approval") val requireApproval: Boolean = false,
)

/**
 * Classification of side effects a step may produce.
 *
 * Used by policies to gate execution. Steps with higher-risk side effect classes
 * may require approval or be blocked entirely.
 *
 * - [READ_ONLY] — no mutations, safe to run without restrictions
 * - [WRITE_SCOPED] — writes within a controlled scope (e.g., a branch, a sandbox)
 * - [EXTERNAL_SIDE_EFFECT] — produces externally visible effects (e.g., sends an email)
 * - [HIGH_RISK] — destructive or irreversible actions (e.g., deleting a resource, merging to main)
 */
@Serializable
enum class SideEffectClass {
    @SerialName("read_only") READ_ONLY,
    @SerialName("write_scoped") WRITE_SCOPED,
    @SerialName("external_side_effect") EXTERNAL_SIDE_EFFECT,
    @SerialName("high_risk") HIGH_RISK,
}

// --- Workflow Template / Version (registry-level) ---

/**
 * A named, reusable workflow template registered in the [org.pekora.registry.WorkflowRegistry].
 *
 * Templates are containers that hold one or more immutable [WorkflowVersion] instances.
 * They provide organizational metadata like ownership and tenant scoping.
 *
 * @property id Unique identifier for this template.
 * @property name Human-readable display name.
 * @property description Optional description of the template's purpose.
 * @property createdAt ISO-8601 timestamp of when the template was registered.
 * @property owner Identifier of the user or team that owns this template.
 * @property tenantId Tenant scope for multi-tenant deployments.
 */
@Serializable
data class WorkflowTemplate(
    val id: String,
    val name: String,
    val description: String = "",
    val createdAt: String = "",
    val owner: String = "",
    val tenantId: String = "",
)

/**
 * An immutable, published version of a [WorkflowTemplate].
 *
 * Once published, a workflow version cannot be modified. New changes require publishing
 * a new version with an incremented version number. Runs are always pinned to a specific
 * version, ensuring reproducibility.
 *
 * @property templateId The ID of the parent [WorkflowTemplate].
 * @property version The version number (monotonically increasing per template).
 * @property definition The complete [WorkflowDefinition] for this version.
 * @property publishedAt ISO-8601 timestamp of when this version was published.
 * @property immutable Always `true` — versions cannot be modified after publication.
 */
@Serializable
data class WorkflowVersion(
    val templateId: String,
    val version: Int,
    val definition: WorkflowDefinition,
    val publishedAt: String = "",
    val immutable: Boolean = true,
)
