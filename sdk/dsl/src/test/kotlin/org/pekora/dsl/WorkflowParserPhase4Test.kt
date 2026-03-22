package org.pekora.dsl

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class WorkflowParserPhase4Test {

    @Test
    fun `parse subworkflow version and parallel fields`() {
        val yaml = """
            workflow:
              name: phase4
              version: 1
              steps:
                - id: fanout
                  type: parallel
                  parallel: [plan_a, plan_b]
                  join_next: merge
                - id: plan_a
                  type: agent
                  agent: planner
                  next: merge
                - id: plan_b
                  type: agent
                  agent: planner
                  next: merge
                - id: merge
                  type: subworkflow
                  subworkflow: child-template
                  subworkflow_version: 2
                  next: done
                - id: done
                  type: result
        """.trimIndent()

        val parsed = WorkflowParser.parse(yaml)
        val parallelStep = parsed.steps.first { it.id == "fanout" }
        val subworkflowStep = parsed.steps.first { it.id == "merge" }

        assertEquals(StepKind.PARALLEL, parallelStep.type)
        assertEquals(listOf("plan_a", "plan_b"), parallelStep.parallel)
        assertEquals("merge", parallelStep.joinNext)

        assertEquals(StepKind.SUBWORKFLOW, subworkflowStep.type)
        assertEquals("child-template", subworkflowStep.subworkflow)
        assertEquals(2, subworkflowStep.subworkflowVersion)
    }
}
