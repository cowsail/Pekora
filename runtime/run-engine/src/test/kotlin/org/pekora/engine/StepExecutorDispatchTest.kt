package org.pekora.engine

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.dispatch.core.DispatchDecision
import org.pekora.dispatch.core.StepDispatchGateway
import org.pekora.dispatch.core.StepDispatchRequest
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepKind
import org.pekora.dsl.StepResultStatus
import org.pekora.policy.PolicyGuard
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals

class StepExecutorDispatchTest {

    private val testKit = ActorTestKit.create()

    @AfterEach
    fun tearDown() {
        testKit.shutdownTestKit()
    }

    @Test
    fun `inline dispatch decision executes adapter and replies with step result`() {
        val adapterInvocations = AtomicInteger(0)
        val executor = testKit.spawn(
            StepExecutor.create(
                agentAdapters = mapOf("langgraph" to CountingAdapter(adapterInvocations)),
                policyGuard = PolicyGuard(),
                stepDispatchGateway = StaticDispatchGateway(
                    DispatchDecision.ExecuteInline(request())
                ),
            )
        )
        val replyProbe = testKit.createTestProbe<RunCommand>()

        executor.tell(
            ExecuteStep(
                request = request(),
                replyTo = replyProbe.ref,
            )
        )

        val result = replyProbe.receiveMessage() as StepResult
        assertEquals("step-1", result.stepId)
        assertEquals(StepResultStatus.SUCCEEDED, result.result.status)
        assertEquals(1, adapterInvocations.get())
    }

    @Test
    fun `queued dispatch decision does not execute adapter inline`() {
        val adapterInvocations = AtomicInteger(0)
        val executor = testKit.spawn(
            StepExecutor.create(
                agentAdapters = mapOf("langgraph" to CountingAdapter(adapterInvocations)),
                policyGuard = PolicyGuard(),
                stepDispatchGateway = StaticDispatchGateway(
                    DispatchDecision.Dispatched("work-item-1")
                ),
            )
        )
        val replyProbe = testKit.createTestProbe<RunCommand>()

        executor.tell(
            ExecuteStep(
                request = request(),
                replyTo = replyProbe.ref,
            )
        )

        replyProbe.expectNoMessage(Duration.ofMillis(250))
        assertEquals(0, adapterInvocations.get())
    }

    private fun request() = StepExecutionRequest(
        runId = "run-1",
        stepId = "step-1",
        stepKind = StepKind.AGENT,
        backend = "langgraph",
        input = mapOf("prompt" to "hello"),
    )
}

private class StaticDispatchGateway(
    private val decision: DispatchDecision,
) : StepDispatchGateway {
    override fun dispatch(request: StepDispatchRequest): CompletionStage<DispatchDecision> =
        CompletableFuture.completedFuture(decision)
}

private class CountingAdapter(
    private val counter: AtomicInteger,
) : AgentRuntimeAdapter {
    override val backendId: String = "langgraph"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        counter.incrementAndGet()
        return CompletableFuture.completedFuture(
            StepExecutionResult(
                status = StepResultStatus.SUCCEEDED,
                output = mapOf("ok" to "true"),
            )
        )
    }
}
