/**
 * Policy evaluation engine for the Pekko Agent Workflow Framework.
 *
 * Evaluates whether a given step is permitted under the currently active policy chain.
 *
 * Policies compose using a **most-restrictive-wins** strategy: when multiple policies
 * apply to the same execution context, the guard merges them by taking the intersection
 * of allowed backends, enforcing approval if *any* policy requires it, and selecting
 * the smallest timeout and token budget across all policies.
 *
 * @see PolicyDefinition
 * @see PolicyDecision
 */
package org.pekora.policy

import org.pekora.dsl.*

/**
 * Evaluates whether steps are permitted under the active policy chain.
 *
 * The guard holds an optional list of global (workflow-level) policies and combines them
 * with any step-level policies supplied at evaluation time. When policies overlap, the
 * most restrictive constraint wins:
 *
 * - **Allowed backends**: a backend must appear in *every* non-empty allowlist to be permitted.
 * - **Approval requirement**: if *any* policy sets [PolicyDefinition.requireApproval],
 *   the resulting [PolicyDecision.requiresApproval] is `true`.
 * - **Timeout / max-tokens**: the *minimum* value across all policies is selected so
 *   that no single policy's budget can be exceeded.
 *
 * @property globalPolicies Workflow-level policies that apply to every step. These are
 *   combined with step-level policies during evaluation. Defaults to an empty list.
 * @see PolicyDecision
 * @see PolicyDefinition
 */
class PolicyGuard(
    private val globalPolicies: List<PolicyDefinition> = emptyList(),
) {
    /**
     * Evaluates whether a step execution is permitted under the combined step-level and
     * global policies.
     *
     * For AGENT steps, checks whether the agent's backend is in [PolicyDefinition.allowedBackends].
     * Also derives the effective approval requirement, timeout, and max-token budget from
     * the merged policy chain.
     *
     * @param step The step definition to evaluate.
     * @param agents A map of agent identifiers to their definitions, used to resolve the
     *   backend for AGENT-type steps.
     * @param stepPolicies Policies scoped to the current step; combined with [globalPolicies].
     * @return A [PolicyDecision] indicating whether execution is allowed, any violations
     *   found, and the effective constraints (approval, timeout, max tokens).
     * @see PolicyDecision
     * @see StepDefinition
     */
    fun evaluate(
        step: StepDefinition,
        agents: Map<String, AgentDefinition>,
        stepPolicies: List<PolicyDefinition>,
    ): PolicyDecision {
        val effectivePolicies = stepPolicies + globalPolicies
        val violations = mutableListOf<String>()

        for (policy in effectivePolicies) {
            if (step.type == StepKind.AGENT && step.agent != null) {
                val agent = agents[step.agent]
                if (agent != null && policy.allowedBackends.isNotEmpty()) {
                    if (agent.backend !in policy.allowedBackends) {
                        violations.add("Backend '${agent.backend}' not in allowed backends: ${policy.allowedBackends}")
                    }
                }
            }
        }

        val requiresApproval = effectivePolicies.any { it.requireApproval }
        val maxTimeout = effectivePolicies.mapNotNull { it.timeoutSeconds }.minOrNull()
        val maxTokens = effectivePolicies.mapNotNull { it.maxTokens }.minOrNull()

        return PolicyDecision(
            allowed = violations.isEmpty(),
            violations = violations,
            requiresApproval = requiresApproval,
            effectiveTimeoutSeconds = maxTimeout,
            effectiveMaxTokens = maxTokens,
        )
    }
}

/**
 * The result of a policy evaluation performed by [PolicyGuard].
 *
 * A [PolicyDecision] captures whether the requested action is permitted, any policy
 * violations that were detected, and the effective runtime constraints derived from
 * the merged policy chain (Section 6.2, Section 14).
 *
 * @property allowed `true` if no policy violations were found; `false` otherwise.
 * @property violations A list of human-readable messages describing each policy violation.
 *   Empty when [allowed] is `true`.
 * @property requiresApproval `true` if any policy in the chain mandates human approval
 *   before execution may proceed.
 * @property effectiveTimeoutSeconds The most restrictive (smallest) timeout in seconds
 *   across all evaluated policies, or `null` if no policy specifies a timeout.
 * @property effectiveMaxTokens The most restrictive (smallest) maximum token budget
 *   across all evaluated policies, or `null` if no policy specifies a token limit.
 * @see PolicyGuard
 */
data class PolicyDecision(
    val allowed: Boolean,
    val violations: List<String> = emptyList(),
    val requiresApproval: Boolean = false,
    val effectiveTimeoutSeconds: Int? = null,
    val effectiveMaxTokens: Long? = null,
)
