package org.pekora.policy

import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class PolicyGuardTest {

    private fun agentStep(agentId: String = "my-agent") = StepDefinition(
        id = "step-1",
        type = StepKind.AGENT,
        agent = agentId,
    )

    private fun agentDef(id: String = "my-agent", backend: String = "langgraph") =
        AgentDefinition(id = id, backend = backend)

    @Test
    fun `allows step when no policies specified`() {
        val guard = PolicyGuard()
        val decision = guard.evaluate(agentStep(), mapOf("my-agent" to agentDef()), emptyList())
        assertTrue(decision.allowed)
        assertTrue(decision.violations.isEmpty())
    }

    @Test
    fun `allows step when backend is in allowed list`() {
        val policy = PolicyDefinition(allowedBackends = listOf("langgraph", "a2a"))
        val guard = PolicyGuard()
        val decision = guard.evaluate(agentStep(), mapOf("my-agent" to agentDef(backend = "langgraph")), listOf(policy))
        assertTrue(decision.allowed)
    }

    @Test
    fun `blocks step when backend is not in allowed list`() {
        val policy = PolicyDefinition(allowedBackends = listOf("a2a"))
        val guard = PolicyGuard()
        val decision = guard.evaluate(agentStep(), mapOf("my-agent" to agentDef(backend = "langgraph")), listOf(policy))
        assertFalse(decision.allowed)
        assertTrue(decision.violations.isNotEmpty())
        assertTrue(decision.violations[0].contains("langgraph"))
    }

    @Test
    fun `most-restrictive-wins step blocked if any policy disallows backend`() {
        val permissive = PolicyDefinition(allowedBackends = listOf("langgraph", "a2a"))
        val restrictive = PolicyDefinition(allowedBackends = listOf("a2a"))
        val guard = PolicyGuard()
        val decision = guard.evaluate(
            agentStep(),
            mapOf("my-agent" to agentDef(backend = "langgraph")),
            listOf(permissive, restrictive),
        )
        assertFalse(decision.allowed)
    }

    @Test
    fun `requiresApproval is true if any policy requires it`() {
        val policy1 = PolicyDefinition(requireApproval = false)
        val policy2 = PolicyDefinition(requireApproval = true)
        val guard = PolicyGuard()
        val decision = guard.evaluate(agentStep(), mapOf("my-agent" to agentDef()), listOf(policy1, policy2))
        assertTrue(decision.requiresApproval)
    }

    @Test
    fun `effectiveTimeoutSeconds is minimum across all policies`() {
        val policy1 = PolicyDefinition(timeoutSeconds = 300)
        val policy2 = PolicyDefinition(timeoutSeconds = 60)
        val guard = PolicyGuard()
        val decision = guard.evaluate(agentStep(), mapOf("my-agent" to agentDef()), listOf(policy1, policy2))
        assertEquals(60, decision.effectiveTimeoutSeconds)
    }

    @Test
    fun `effectiveMaxTokens is minimum across all policies`() {
        val policy1 = PolicyDefinition(maxTokens = 10000)
        val policy2 = PolicyDefinition(maxTokens = 4096)
        val guard = PolicyGuard()
        val decision = guard.evaluate(agentStep(), mapOf("my-agent" to agentDef()), listOf(policy1, policy2))
        assertEquals(4096L, decision.effectiveMaxTokens)
    }

    @Test
    fun `global policies combine with step policies`() {
        val globalPolicy = PolicyDefinition(allowedBackends = listOf("a2a"))
        val guard = PolicyGuard(globalPolicies = listOf(globalPolicy))
        // No step-level policies, but global blocks langgraph
        val decision = guard.evaluate(agentStep(), mapOf("my-agent" to agentDef(backend = "langgraph")), emptyList())
        assertFalse(decision.allowed)
    }

    @Test
    fun `non-AGENT steps are not subject to backend policy check`() {
        val policy = PolicyDefinition(allowedBackends = listOf("a2a"))
        val guard = PolicyGuard()
        val approvalStep = StepDefinition(id = "approve", type = StepKind.APPROVAL)
        val decision = guard.evaluate(approvalStep, emptyMap(), listOf(policy))
        assertTrue(decision.allowed)
    }
}
