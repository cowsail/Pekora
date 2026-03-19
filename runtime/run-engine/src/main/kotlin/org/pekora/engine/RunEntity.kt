/**
 * # RunEntity — Event-Sourced Workflow Run Actor
 *
 * This file defines [RunEntity], the event-sourced persistent actor that serves as the
 * canonical runtime owner for a single workflow run (Section 6.1 of the architecture spec).
 *
 * Each [RunEntity] instance is identified by a unique `runId` and is managed via Apache Pekko
 * Cluster Sharding, ensuring that exactly one instance exists for each run across the entire
 * cluster. The entity follows the event sourcing pattern: all state mutations are expressed as
 * [RunEvent] instances that are persisted to the journal before being applied to the in-memory
 * [RunState]. This guarantees durability, replayability, and crash recovery.
 *
 * ## Lifecycle
 *
 * 1. **CreateRun** — Initializes metadata (template, version, inputs, tenant).
 * 2. **LoadWorkflow** — Attaches a parsed [WorkflowDefinition][org.pekora.dsl.WorkflowDefinition].
 * 3. **StartRun** — Transitions to EXECUTING and schedules the first step.
 * 4. Steps are executed via the [StepExecutor] child actor; results flow back as [StepResult] commands.
 * 5. Approval gates pause the run and delegate to the [ApprovalManager].
 * 6. The run completes, fails, or is cancelled depending on step outcomes and external commands.
 *
 * ## Internal Commands
 *
 * [CompleteRunInternal] and [RequestApprovalInternal] are internal-only commands that the
 * entity sends to itself during workflow advancement. They are not part of the public
 * actor protocol.
 *
 * @see RunCommand
 * @see RunEvent
 * @see RunState
 * @see StepExecutor
 * @see ApprovalManager
 * @see RunEntityTypeKey
 */
package org.pekora.engine

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.javadsl.*
import org.pekora.dsl.*

/**
 * The event-sourced persistent actor that owns a single workflow run.
 *
 * `RunEntity` is the canonical runtime owner described in Section 6.1 of the architecture.
 * It is keyed by `runId` and sharded across the cluster via [RunEntityTypeKey]. All state
 * changes are driven by persisted [RunEvent] instances, making the actor fully recoverable
 * after restarts or migrations.
 *
 * The actor accepts [RunCommand] messages, validates them against the current [RunState],
 * persists the resulting [RunEvent](s), and then performs side effects such as scheduling
 * the next step or forwarding approval requests.
 *
 * @property runId The unique identifier for this workflow run, used as the sharding entity ID.
 * @property persistenceId The Pekko Persistence identity, typically derived from the entity type and `runId`.
 * @property ctx The typed actor context providing access to the actor system, self-reference, and child spawning.
 * @property stepExecutor A reference to the [StepExecutor] actor responsible for dispatching step execution to adapters.
 * @property approvalManager A reference to the [ApprovalManager] actor that tracks and routes approval workflows.
 *
 * @see RunCommand
 * @see RunEvent
 * @see RunState
 * @see RunEntityTypeKey
 */
class RunEntity private constructor(
    private val runId: String,
    private val persistenceId: PersistenceId,
    private val ctx: ActorContext<RunCommand>,
    private val stepExecutor: ActorRef<StepExecutorMessage>,
    private val approvalManager: ActorRef<ApprovalCommand>,
) : EventSourcedBehavior<RunCommand, RunEvent, RunState>(persistenceId) {

    /**
     * Companion object providing the factory method and entity type constant for [RunEntity].
     */
    companion object {
        /**
         * The string key used to register this entity type with Pekko Cluster Sharding.
         * This value is referenced by [RunEntityTypeKey] to create the [EntityTypeKey].
         */
        const val ENTITY_TYPE_KEY = "RunEntity"

        /**
         * Factory method that creates a new [RunEntity] behavior wrapped in a [Behaviors.setup] block.
         *
         * This is the standard Pekko Typed pattern for actors that need access to the
         * [ActorContext] during construction. Cluster Sharding calls this factory each time
         * it needs to instantiate or recover a `RunEntity` on a given node.
         *
         * @param runId The unique identifier for this workflow run (also the sharding entity ID).
         * @param persistenceId The Pekko Persistence identity used for journal and snapshot storage.
         * @param stepExecutor A reference to the [StepExecutor] actor for dispatching step execution.
         * @param approvalManager A reference to the [ApprovalManager] actor for handling approval gates.
         * @return A [Behavior] that, when materialized, produces a fully initialized [RunEntity].
         *
         * @see RunEntityTypeKey
         */
        fun create(
            runId: String,
            persistenceId: PersistenceId,
            stepExecutor: ActorRef<StepExecutorMessage>,
            approvalManager: ActorRef<ApprovalCommand>,
        ): Behavior<RunCommand> = Behaviors.setup { ctx ->
            RunEntity(runId, persistenceId, ctx, stepExecutor, approvalManager)
        }
    }

    /**
     * Returns the initial empty state for a newly created or recovered entity.
     *
     * When the entity is first instantiated (no events in the journal) this state
     * serves as the starting point. During recovery, events are replayed on top of
     * this empty state via [RunState.applyEvent].
     *
     * @return A [RunState] with status [CREATED][org.pekora.dsl.RunState.CREATED] and no definition loaded.
     */
    override fun emptyState(): RunState = RunState.empty(runId)

    /**
     * Builds the command handler that maps incoming [RunCommand] messages to persistence effects.
     *
     * Each command handler validates preconditions against the current [RunState], then either:
     * - Persists one or more [RunEvent] instances and schedules post-persist side effects, or
     * - Returns [Effect.none] if the command is invalid or a no-op (e.g., duplicate create).
     *
     * The handler is registered for all states (`forAnyState`), since the individual handler
     * methods perform their own state-specific validation.
     *
     * @return A [CommandHandler] covering all [RunCommand] subtypes accepted by this entity.
     *
     * @see eventHandler
     */
    override fun commandHandler(): CommandHandler<RunCommand, RunEvent, RunState> =
        newCommandHandlerBuilder()
            .forAnyState()
            .onCommand(CreateRun::class.java, this::onCreateRun)
            .onCommand(LoadWorkflow::class.java, this::onLoadWorkflow)
            .onCommand(StartRun::class.java, this::onStartRun)
            .onCommand(StepResult::class.java, this::onStepResult)
            .onCommand(ApprovalResponse::class.java, this::onApprovalResponse)
            .onCommand(CancelRun::class.java, this::onCancelRun)
            .onCommand(ResumeRun::class.java, this::onResumeRun)
            .onCommand(GetRunStatus::class.java, this::onGetRunStatus)
            .onCommand(RequestApprovalInternal::class.java, this::onRequestApprovalInternal)
            .onCommand(CompleteRunInternal::class.java, this::onCompleteRunInternal)
            .build()

    /**
     * Builds the event handler that applies persisted [RunEvent] instances to the [RunState].
     *
     * During both normal operation (after persisting a new event) and recovery (replaying
     * the journal), each event is passed to [RunState.applyEvent] to produce the next state.
     * This handler is registered for all states (`forAnyState`), and each event type is
     * individually mapped to the state's `applyEvent` method.
     *
     * @return An [EventHandler] covering all [RunEvent] subtypes that this entity can produce.
     *
     * @see commandHandler
     * @see RunState.applyEvent
     */
    override fun eventHandler(): EventHandler<RunState, RunEvent> =
        newEventHandlerBuilder()
            .forAnyState()
            .onEvent(RunCreated::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(WorkflowLoaded::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunStarted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepScheduled::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepStarted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepCompleted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepFailed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepRetryScheduled::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ApprovalRequested::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ApprovalReceived::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunPaused::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunResumed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunCompleted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunFailed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunCancelled::class.java) { state, event -> state.applyEvent(event) }
            .build()

    // --- Command Handlers ---

    private fun onCreateRun(state: RunState, cmd: CreateRun): Effect<RunEvent, RunState> {
        if (state.status != org.pekora.dsl.RunState.CREATED || state.definition != null) {
            cmd.replyTo.tell(RunCommandResponse(false, "Run already created"))
            return Effect().none()
        }
        val event = RunCreated(
            runId = runId,
            templateId = cmd.templateId,
            versionNumber = cmd.versionNumber,
            inputs = cmd.inputs,
            tenantId = cmd.tenantId,
            correlationId = cmd.correlationId,
        )
        return Effect().persist(event).thenRun { _: RunState ->
            cmd.replyTo.tell(RunCommandResponse(true, "Run created"))
        }
    }

    private fun onLoadWorkflow(state: RunState, cmd: LoadWorkflow): Effect<RunEvent, RunState> {
        val event = WorkflowLoaded(runId = runId, definition = cmd.definition)
        return Effect().persist(event).thenRun { _: RunState ->
            cmd.replyTo.tell(RunCommandResponse(true, "Workflow loaded"))
        }
    }

    private fun onStartRun(state: RunState, cmd: StartRun): Effect<RunEvent, RunState> {
        if (state.definition == null) {
            cmd.replyTo.tell(RunCommandResponse(false, "Workflow not loaded"))
            return Effect().none()
        }
        if (state.status != org.pekora.dsl.RunState.READY) {
            cmd.replyTo.tell(RunCommandResponse(false, "Run not in READY state: ${state.status}"))
            return Effect().none()
        }
        val event = RunStarted(runId = runId)
        return Effect().persist(event).thenRun { newState: RunState ->
            cmd.replyTo.tell(RunCommandResponse(true, "Run started"))
            scheduleNextStep(newState)
        }
    }

    private fun onStepResult(state: RunState, cmd: StepResult): Effect<RunEvent, RunState> {
        return if (cmd.result.status == StepResultStatus.SUCCEEDED) {
            val event = StepCompleted(
                runId = runId,
                stepId = cmd.stepId,
                output = cmd.result.output,
                toolCalls = cmd.result.toolCalls,
                metrics = cmd.result.metrics,
            )
            Effect().persist(event).thenRun { newState: RunState ->
                advanceWorkflow(newState, cmd.stepId)
            }
        } else {
            val stepDef = state.definition?.steps?.find { it.id == cmd.stepId }
            val retryConfig = stepDef?.retries
            val currentAttempt = state.stepAttempts[cmd.stepId] ?: 1

            if (retryConfig != null && currentAttempt < retryConfig.maxAttempts) {
                val retryEvent = StepRetryScheduled(
                    runId = runId,
                    stepId = cmd.stepId,
                    attempt = currentAttempt + 1,
                    nextRetryAt = System.currentTimeMillis() +
                        (retryConfig.backoffMs * Math.pow(retryConfig.multiplier, (currentAttempt - 1).toDouble())).toLong(),
                )
                Effect().persist(retryEvent).thenRun { newState: RunState ->
                    scheduleStepExecution(newState, cmd.stepId)
                }
            } else {
                val failEvent = StepFailed(
                    runId = runId,
                    stepId = cmd.stepId,
                    error = cmd.result.error ?: "Unknown error",
                    retryable = false,
                )
                val runFailEvent = RunFailed(runId = runId, error = "Step ${cmd.stepId} failed: ${cmd.result.error}")
                Effect().persist(listOf(failEvent, runFailEvent)).thenRun { _: RunState -> }
            }
        }
    }

    private fun onApprovalResponse(state: RunState, cmd: ApprovalResponse): Effect<RunEvent, RunState> {
        val event = ApprovalReceived(
            runId = runId,
            stepId = cmd.stepId,
            approvalId = cmd.approvalId,
            approved = cmd.approved,
            approver = cmd.approver,
            reason = cmd.reason,
        )
        return Effect().persist(event).thenRun { newState: RunState ->
            if (cmd.approved) {
                advanceWorkflow(newState, cmd.stepId)
            }
        }
    }

    private fun onCancelRun(state: RunState, cmd: CancelRun): Effect<RunEvent, RunState> {
        val event = RunCancelled(runId = runId, reason = cmd.reason)
        return Effect().persist(event).thenRun { _: RunState ->
            cmd.replyTo.tell(RunCommandResponse(true, "Run cancelled"))
        }
    }

    private fun onResumeRun(state: RunState, cmd: ResumeRun): Effect<RunEvent, RunState> {
        val event = RunResumed(runId = runId)
        return Effect().persist(event).thenRun { newState: RunState ->
            cmd.replyTo.tell(RunCommandResponse(true, "Run resumed"))
            scheduleNextStep(newState)
        }
    }

    private fun onGetRunStatus(state: RunState, cmd: GetRunStatus): Effect<RunEvent, RunState> {
        cmd.replyTo.tell(
            RunStatusResponse(
                runId = runId,
                status = state.status,
                stepStates = state.stepStates.toMap(),
                outputs = state.outputs.toMap(),
                stepToolCalls = state.stepToolCalls.toMap(),
                error = state.error,
            )
        )
        return Effect().none()
    }

    private fun onRequestApprovalInternal(state: RunState, cmd: RequestApprovalInternal): Effect<RunEvent, RunState> {
        val event = ApprovalRequested(
            runId = runId,
            stepId = cmd.stepId,
            approvalId = cmd.approvalId,
            approvers = cmd.approvers,
        )
        return Effect().persist(event).thenRun { _: RunState ->
            approvalManager.tell(
                RequestApproval(
                    approvalId = cmd.approvalId,
                    runId = runId,
                    stepId = cmd.stepId,
                    approvers = cmd.approvers,
                    runEntity = ctx.self,
                )
            )
        }
    }

    private fun onCompleteRunInternal(state: RunState, cmd: CompleteRunInternal): Effect<RunEvent, RunState> {
        val event = RunCompleted(runId = runId, output = cmd.outputs)
        return Effect().persist(event).thenRun { _: RunState -> }
    }

    // --- Workflow Advancement ---

    private fun scheduleNextStep(state: RunState) {
        val definition = state.definition ?: return
        val firstStep = definition.steps.firstOrNull() ?: return
        if (state.stepStates[firstStep.id] == null) {
            scheduleStepExecution(state, firstStep.id)
        }
    }

    private fun advanceWorkflow(state: RunState, completedStepId: String) {
        val definition = state.definition ?: return
        val completedStep = definition.steps.find { it.id == completedStepId } ?: return

        val nextStepId = completedStep.next
        if (nextStepId == null) {
            if (completedStep.type == StepKind.RESULT) {
                ctx.self.tell(CompleteRunInternal(state.outputs.toMap()))
            }
            return
        }

        val nextStep = definition.steps.find { it.id == nextStepId }
        if (nextStep != null) {
            scheduleStepExecution(state, nextStep.id)
        }
    }

    private fun scheduleStepExecution(state: RunState, stepId: String) {
        val definition = state.definition ?: return
        val step = definition.steps.find { it.id == stepId } ?: return

        val resolvedInput = resolveInputExpressions(step.input, state)

        when (step.type) {
            StepKind.APPROVAL -> {
                val approvalId = "approval_${runId}_${stepId}"
                ctx.self.tell(RequestApprovalInternal(stepId, approvalId, step.approvers))
            }
            StepKind.RESULT -> {
                val resolvedOutput = resolveInputExpressions(step.output, state)
                stepExecutor.tell(ExecuteResultStep(runId, stepId, resolvedOutput, ctx.self))
            }
            else -> {
                val agents = definition.agents.associateBy { it.id }
                val backend = if (step.agent != null) agents[step.agent]?.backend ?: "native" else "native"

                val request = StepExecutionRequest(
                    runId = runId,
                    stepId = stepId,
                    stepKind = step.type,
                    backend = backend,
                    definitionRef = step.agent ?: "",
                    input = resolvedInput,
                    constraints = StepConstraints(
                        timeoutSeconds = step.timeout ?: 300,
                    ),
                )
                val workflowPolicies = definition.policies.mapNotNull { it.inline }
                stepExecutor.tell(ExecuteStep(
                    request = request,
                    replyTo = ctx.self,
                    stepDefinition = step,
                    agents = agents,
                    stepPolicies = workflowPolicies,
                ))
            }
        }
    }

    private fun resolveInputExpressions(
        input: Map<String, String>,
        state: RunState,
    ): Map<String, String> {
        val expressionPattern = Regex("""\$\{([^}]+)}""")
        return input.mapValues { (_, value) ->
            expressionPattern.replace(value) { match ->
                val path = match.groupValues[1]
                resolveExpression(path, state)
            }
        }
    }

    private fun resolveExpression(path: String, state: RunState): String {
        val parts = path.split(".")
        return when {
            parts[0] == "inputs" && parts.size >= 2 -> {
                state.inputs[parts[1]] ?: ""
            }
            parts[0] == "steps" && parts.size >= 3 -> {
                val stepId = parts[1]
                val stepOutput = state.stepOutputs[stepId] ?: emptyMap()
                if (parts.size >= 4 && parts[2] == "output") {
                    stepOutput[parts[3]] ?: ""
                } else if (parts[2] == "output") {
                    stepOutput.toString()
                } else {
                    ""
                }
            }
            else -> ""
        }
    }
}

/**
 * Internal command sent by [RunEntity] to itself when all steps in the workflow have
 * completed and the run should transition to the [COMPLETED][org.pekora.dsl.RunState.COMPLETED] state.
 *
 * This command is not part of the public actor protocol and must not be sent by external callers.
 *
 * @property outputs The final aggregated output map to persist with the [RunCompleted] event.
 */
internal data class CompleteRunInternal(val outputs: Map<String, String>) : RunCommand

/**
 * Internal command sent by [RunEntity] to itself when a step of type [APPROVAL][StepKind.APPROVAL]
 * is encountered during workflow advancement. This triggers persisting an [ApprovalRequested] event
 * and forwarding the request to the [ApprovalManager].
 *
 * This command is not part of the public actor protocol and must not be sent by external callers.
 *
 * @property stepId The identifier of the approval step in the workflow definition.
 * @property approvalId A unique identifier for this approval request, typically `"approval_{runId}_{stepId}"`.
 * @property approvers The list of approver identifiers who are authorized to grant or deny this approval.
 */
internal data class RequestApprovalInternal(
    val stepId: String,
    val approvalId: String,
    val approvers: List<String>,
) : RunCommand
