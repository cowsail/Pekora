package org.pekora.dispatch.core

import org.pekora.dsl.StepExecutionResult
import java.util.concurrent.CompletionStage

interface StepResultSink {
    fun submit(
        runId: String,
        stepId: String,
        attempt: Int,
        result: StepExecutionResult,
    ): CompletionStage<Unit>
}
