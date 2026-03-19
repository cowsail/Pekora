package org.pekora.engine

import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import org.pekora.policy.PolicyGuard
import kotlin.test.assertEquals

/**
 * Tests policy enforcement logic used by StepExecutor before adapter dispatch.
 *
 * Full actor-based StepExecutor tests (dispatch, unknown backend) require an ActorSystem
 * and are covered by integration tests.
 */
class StepExecutorTest {

    private fun agentStep(agentId: String = "agent-1") = StepDefinition(
        id = "step-1",
        type = StepKind.AGENT,
        agent = agentId,
    )

    private fun agentDef(id: String = "agent-1", backend: String = "langgraph") =
        AgentDefinition(id = id, backend = backend)

    @Test
    fun `PolicyGuard blocks disallowed backend before adapter dispatch`() {
        val policy = PolicyDefinition(allowedBackends = listOf("a2a"))
        val guard = PolicyGuard()
        val step = agentStep()
        val agents = mapOf("agent-1" to agentDef(backend = "langgraph"))

        val decision = guard.evaluate(step, agents, listOf(policy))
        assertEquals(false, decision.allowed)
        assertEquals(1, decision.violations.size)
        assertTrue(decision.violations[0].contains("langgraph"))
    }

    @Test
    fun `PolicyGuard allows step with matching backend`() {
        val policy = PolicyDefinition(allowedBackends = listOf("langgraph"))
        val guard = PolicyGuard()
        val step = agentStep()
        val agents = mapOf("agent-1" to agentDef(backend = "langgraph"))

        val decision = guard.evaluate(step, agents, listOf(policy))
        assertEquals(true, decision.allowed)
    }

    @Test
    fun `PolicyGuard allows step with no backend restrictions`() {
        val policy = PolicyDefinition()
        val guard = PolicyGuard()
        val step = agentStep()
        val agents = mapOf("agent-1" to agentDef(backend = "any-backend"))

        val decision = guard.evaluate(step, agents, listOf(policy))
        assertEquals(true, decision.allowed)
    }

    @Test
    fun `PolicyGuard with global policies blocks disallowed backend`() {
        val globalPolicy = PolicyDefinition(allowedBackends = listOf("a2a"))
        val guard = PolicyGuard(globalPolicies = listOf(globalPolicy))
        val step = agentStep()
        val agents = mapOf("agent-1" to agentDef(backend = "langgraph"))

        val decision = guard.evaluate(step, agents, emptyList())
        assertEquals(false, decision.allowed)
    }
}

private fun assertTrue(value: Boolean) = kotlin.test.assertTrue(value)
