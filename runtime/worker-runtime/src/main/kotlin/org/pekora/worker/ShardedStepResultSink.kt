package org.pekora.worker

import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.pekora.dispatch.core.StepResultSink
import org.pekora.dsl.StepExecutionResult
import org.pekora.engine.RunEntityTypeKey
import org.pekora.engine.StepResult
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

class ShardedStepResultSink(
    private val sharding: ClusterSharding,
) : StepResultSink {
    override fun submit(
        runId: String,
        stepId: String,
        attempt: Int,
        result: StepExecutionResult,
    ): CompletionStage<Unit> {
        val runRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
        runRef.tell(
            StepResult(
                stepId = stepId,
                attempt = attempt,
                result = result,
            )
        )
        return CompletableFuture.completedFuture(Unit)
    }
}
