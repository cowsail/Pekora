package org.pekora.dispatch.core

import org.pekora.dsl.AgentDefinition
import org.pekora.dsl.PolicyDefinition
import org.pekora.dsl.StepDefinition
import org.pekora.dsl.StepExecutionRequest
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

data class StepDispatchRequest(
    val request: StepExecutionRequest,
    val stepDefinition: StepDefinition? = null,
    val agents: Map<String, AgentDefinition> = emptyMap(),
    val stepPolicies: List<PolicyDefinition> = emptyList(),
)

sealed interface DispatchDecision {
    val mode: DispatchMode

    data class ExecuteInline(
        val executionRequest: StepExecutionRequest,
    ) : DispatchDecision {
        override val mode: DispatchMode = DispatchMode.INLINE
    }

    data class Dispatched(
        val workItemId: String,
    ) : DispatchDecision {
        override val mode: DispatchMode = DispatchMode.QUEUE
    }
}

interface StepDispatchGateway {
    fun dispatch(request: StepDispatchRequest): CompletionStage<DispatchDecision>
}

class InlineStepDispatchGateway : StepDispatchGateway {
    override fun dispatch(request: StepDispatchRequest): CompletionStage<DispatchDecision> =
        CompletableFuture.completedFuture(DispatchDecision.ExecuteInline(request.request))
}
