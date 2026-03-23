package org.pekora.engine

import org.junit.jupiter.api.Test
import org.pekora.dsl.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class RunStatePhase4Test {

    @Test
    fun `parallel group events track pending and output`() {
        val runId = "run-1"
        var state = RunState.empty(runId)

        state = state.applyEvent(
            ParallelGroupStarted(
                runId = runId,
                parallelStepId = "fanout",
                branches = listOf("a", "b"),
                joinStepId = "join",
            )
        )
        assertNotNull(state.parallelGroups["fanout"])
        assertEquals(2, state.parallelGroups["fanout"]?.pendingBranches?.size)

        state = state.applyEvent(
            ParallelBranchCompleted(
                runId = runId,
                parallelStepId = "fanout",
                branchRootStepId = "a",
                branchOutput = mapOf("message" to "ok"),
            )
        )
        assertEquals(1, state.parallelGroups["fanout"]?.pendingBranches?.size)

        state = state.applyEvent(
            ParallelGroupCompleted(
                runId = runId,
                parallelStepId = "fanout",
                output = mapOf("a.message" to "ok"),
            )
        )

        assertEquals(StepState.SUCCEEDED, state.stepStates["fanout"])
        assertEquals("ok", state.stepOutputs["fanout"]?.get("a.message"))
        assertTrue(state.parallelGroups.isEmpty())
    }

    @Test
    fun `subworkflow lifecycle events update child tracking`() {
        val runId = "run-2"
        var state = RunState.empty(runId)

        state = state.applyEvent(
            SubworkflowChildStarted(
                runId = runId,
                stepId = "child-step",
                childRunId = "run-2__child-step__1",
                templateId = "child-template",
                versionNumber = 1,
            )
        )

        assertEquals(StepState.RUNNING, state.stepStates["child-step"])
        assertEquals(org.pekora.dsl.RunState.EXECUTING, state.subworkflowChildren["child-step"]?.status)

        state = state.applyEvent(
            SubworkflowChildCompleted(
                runId = runId,
                stepId = "child-step",
                childRunId = "run-2__child-step__1",
                output = mapOf("result" to "done"),
            )
        )

        assertEquals(org.pekora.dsl.RunState.COMPLETED, state.subworkflowChildren["child-step"]?.status)
        assertEquals("done", state.subworkflowChildren["child-step"]?.output?.get("result"))
    }
}
