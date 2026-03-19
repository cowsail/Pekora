/**
 * Policy evaluation engine for the Pekko Agent Workflow Framework.
 *
 * This file implements the policy guard described in **Section 6.2** (Policy Model) and
 * **Section 14** (Policy Enforcement) of the framework specification. The [PolicyGuard]
 * evaluates whether a given step, tool call, or skill call is permitted under the
 * currently active policy chain.
 *
 * Policies compose using a **most-restrictive-wins** strategy: when multiple policies
 * apply to the same execution context, the guard merges them by taking the intersection
 * of allowed backends/tools/skills, enforcing approval if *any* policy requires it,
 * and selecting the smallest timeout and token budget across all policies.
 *
 * @see PolicyDefinition
 * @see PolicyDecision
 */
package org.pekora.policy

import org.pekora.dsl.*
import kotlin.collections.get
import kotlin.text.contains

/**
 * Evaluates whether steps, tool calls, and skill calls are permitted under the active
 * policy chain (Section 6.2, Section 14).
 *
 * The guard holds an optional list of global (workflow-level) policies and combines them
 * with any step-level policies supplied at evaluation time. When policies overlap, the
 * most restrictive constraint wins:
 *
 * - **Allowed backends / tools / skills**: a resource must appear in *every* non-empty
 *   allowlist to be considered permitted.
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
     * Depending on [StepDefinition.type], the method checks:
     * - **AGENT** steps: whether the agent's backend is in [PolicyDefinition.allowedBackends].
     * - **TOOL** steps: whether the tool identifier is in [PolicyDefinition.allowedTools].
     * - **SKILL** steps: whether the skill identifier is in [PolicyDefinition.allowedSkills].
     *
     * After collecting violations the method also derives the effective approval
     * requirement, timeout, and max-token budget from the merged policy chain.
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
            // Check backend
            if (step.type == StepKind.AGENT && step.agent != null) {
                val agent = agents[step.agent]
                if (agent != null && policy.allowedBackends.isNotEmpty()) {
                    if (agent.backend !in policy.allowedBackends) {
                        violations.add("Backend '${agent.backend}' not in allowed backends: ${policy.allowedBackends}")
                    }
                }
            }

            // Check tool
            if (step.type == StepKind.TOOL && step.tool != null) {
                if (policy.allowedTools.isNotEmpty() && step.tool !in policy.allowedTools) {
                    violations.add("Tool '${step.tool}' not in allowed tools: ${policy.allowedTools}")
                }
            }

            // Check skill
            if (step.type == StepKind.SKILL && step.skill != null) {
                if (policy.allowedSkills.isNotEmpty() && step.skill !in policy.allowedSkills) {
                    violations.add("Skill '${step.skill}' not in allowed skills: ${policy.allowedSkills}")
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

    /**
     * Evaluates a tool invocation request against the combined step-level and global policies.
     *
     * Each policy with a non-empty [PolicyDefinition.allowedTools] list is checked; if the
     * given [toolId] does not appear in any such list, a violation is recorded.
     *
     * @param toolId The identifier of the tool being invoked.
     * @param policies Step-level policies to merge with [globalPolicies].
     * @return A [PolicyDecision] with [PolicyDecision.allowed] set to `false` if the tool
     *   is blocked by any policy, along with descriptive violation messages.
     * @see PolicyDecision
     */
    fun evaluateToolCall(toolId: String, policies: List<PolicyDefinition>): PolicyDecision {
        val effectivePolicies = policies + globalPolicies
        val violations = mutableListOf<String>()

        for (policy in effectivePolicies) {
            if (policy.allowedTools.isNotEmpty() && toolId !in policy.allowedTools) {
                violations.add("Tool '$toolId' not allowed by policy '${policy.id}'")
            }
        }

        return PolicyDecision(
            allowed = violations.isEmpty(),
            violations = violations,
        )
    }

    /**
     * Evaluates a skill invocation request against the combined step-level and global policies.
     *
     * Each policy with a non-empty [PolicyDefinition.allowedSkills] list is checked; if
     * the given [skillId] does not appear in any such list, a violation is recorded.
     *
     * @param skillId The identifier of the skill being invoked.
     * @param policies Step-level policies to merge with [globalPolicies].
     * @return A [PolicyDecision] with [PolicyDecision.allowed] set to `false` if the skill
     *   is blocked by any policy, along with descriptive violation messages.
     * @see PolicyDecision
     */
    fun evaluateSkillCall(skillId: String, policies: List<PolicyDefinition>): PolicyDecision {
        val effectivePolicies = policies + globalPolicies
        val violations = mutableListOf<String>()

        for (policy in effectivePolicies) {
            if (policy.allowedSkills.isNotEmpty() && skillId !in policy.allowedSkills) {
                violations.add("Skill '$skillId' not allowed by policy '${policy.id}'")
            }
        }

        return PolicyDecision(
            allowed = violations.isEmpty(),
            violations = violations,
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
