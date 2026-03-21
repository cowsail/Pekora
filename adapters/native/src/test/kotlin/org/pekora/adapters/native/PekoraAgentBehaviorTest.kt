package org.pekora.adapters.native

import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.pekora.adapters.generic.GenericActorRequest
import org.pekora.dsl.*
import kotlin.test.assertEquals

class PekoraAgentBehaviorTest {

    companion object {
        private val testKit = ActorTestKit.create()

        @JvmStatic
        @AfterAll
        fun tearDown() = testKit.shutdownTestKit()
    }

    private fun makeRequest(stepId: String = "step-1", input: Map<String, String> = emptyMap()) =
        StepExecutionRequest(
            runId = "run-1",
            stepId = stepId,
            stepKind = StepKind.AGENT,
            backend = "native",
            definitionRef = "echo",
            input = input,
        )

    // A trivial echo agent that returns the input as output
    private class EchoAgent(context: ActorContext<GenericActorRequest>) : PekoraAgentBehavior(context) {
        override fun handleStep(request: StepExecutionRequest) =
            StepExecutionResult(status = StepResultStatus.SUCCEEDED, output = request.input)
    }

    // An agent that always throws
    private class ThrowingAgent(context: ActorContext<GenericActorRequest>) : PekoraAgentBehavior(context) {
        override fun handleStep(request: StepExecutionRequest): StepExecutionResult =
            throw RuntimeException("deliberate failure")
    }

    @Test
    fun `echo agent returns input as output`() {
        val actor = testKit.spawn(PekoraAgentBehavior.create(::EchoAgent))
        val probe = testKit.createTestProbe(StepExecutionResult::class.java)

        actor.tell(GenericActorRequest(makeRequest(input = mapOf("text" to "hello")), probe.ref))

        val result = probe.receiveMessage()
        assertEquals(StepResultStatus.SUCCEEDED, result.status)
        assertEquals("hello", result.output["text"])
    }

    @Test
    fun `agent that throws returns FAILED result without crashing actor`() {
        val actor = testKit.spawn(PekoraAgentBehavior.create(::ThrowingAgent))
        val probe = testKit.createTestProbe(StepExecutionResult::class.java)

        actor.tell(GenericActorRequest(makeRequest(), probe.ref))
        val result = probe.receiveMessage()
        assertEquals(StepResultStatus.FAILED, result.status)
        assertEquals("deliberate failure", result.error)

        // Actor is still alive — next message is processed normally
        val echoActor = testKit.spawn(PekoraAgentBehavior.create(::EchoAgent))
        echoActor.tell(GenericActorRequest(makeRequest(input = mapOf("k" to "v")), probe.ref))
        val result2 = probe.receiveMessage()
        assertEquals(StepResultStatus.SUCCEEDED, result2.status)
    }
}
