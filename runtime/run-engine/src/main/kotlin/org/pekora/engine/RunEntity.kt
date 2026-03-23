package org.pekora.engine

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.javadsl.*
import org.pekora.dsl.*
import org.pekora.registry.GetVersion
import org.pekora.registry.RegistryCommand
import org.pekora.registry.VersionResponse
import java.time.Duration

class RunEntity private constructor(
    private val runId: String,
    private val persistenceId: PersistenceId,
    private val ctx: ActorContext<RunCommand>,
    private val stepExecutor: ActorRef<StepExecutorMessage>,
    private val approvalManager: ActorRef<ApprovalCommand>,
    private val registry: ActorRef<RegistryCommand>,
    private val sharding: ClusterSharding,
) : EventSourcedBehavior<RunCommand, RunEvent, RunState>(persistenceId) {

    companion object {
        const val ENTITY_TYPE_KEY = "RunEntity"

        fun create(
            runId: String,
            persistenceId: PersistenceId,
            stepExecutor: ActorRef<StepExecutorMessage>,
            approvalManager: ActorRef<ApprovalCommand>,
            registry: ActorRef<RegistryCommand>,
            sharding: ClusterSharding,
        ): Behavior<RunCommand> = Behaviors.setup { ctx ->
            RunEntity(runId, persistenceId, ctx, stepExecutor, approvalManager, registry, sharding)
        }
    }

    private val childCommandAckAdapter: ActorRef<RunCommandResponse> =
        ctx.messageAdapter(RunCommandResponse::class.java) { IgnoreChildCommandAckInternal }

    override fun emptyState(): RunState = RunState.empty(runId)

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
            .onCommand(StartParallelInternal::class.java, this::onStartParallel)
            .onCommand(ParallelBranchTerminalInternal::class.java, this::onParallelBranchTerminal)
            .onCommand(CheckParallelFanInInternal::class.java, this::onCheckParallelFanIn)
            .onCommand(StartSubworkflowInternal::class.java, this::onStartSubworkflow)
            .onCommand(SubworkflowVersionResolvedInternal::class.java, this::onSubworkflowVersionResolved)
            .onCommand(PollSubworkflowStatusInternal::class.java, this::onPollSubworkflowStatus)
            .onCommand(SubworkflowStatusResponseInternal::class.java, this::onSubworkflowStatusResponse)
            .onCommand(EvaluateDecisionInternal::class.java, this::onEvaluateDecision)
            .onCommand(IgnoreChildCommandAckInternal::class.java) { _, _ -> Effect().none() }
            .build()

    override fun eventHandler(): EventHandler<RunState, RunEvent> =
        newEventHandlerBuilder()
            .forAnyState()
            .onEvent(RunCreated::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(WorkflowLoaded::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunStarted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepScheduled::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepStarted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepCompleted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ParallelGroupStarted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ParallelBranchCompleted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ParallelBranchFailed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ParallelGroupCompleted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ParallelGroupFailed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepFailed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(StepRetryScheduled::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(SubworkflowChildStarted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(SubworkflowChildCompleted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(SubworkflowChildFailed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ApprovalRequested::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(ApprovalReceived::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunPaused::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunResumed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunCompleted::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunFailed::class.java) { state, event -> state.applyEvent(event) }
            .onEvent(RunCancelled::class.java) { state, event -> state.applyEvent(event) }
            .build()

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
        return Effect().persist(event).thenRun {
            cmd.replyTo.tell(RunCommandResponse(true, "Run created"))
        }
    }

    private fun onLoadWorkflow(state: RunState, cmd: LoadWorkflow): Effect<RunEvent, RunState> {
        val validationError = validateWorkflow(cmd.definition)
        if (validationError != null) {
            cmd.replyTo.tell(RunCommandResponse(false, validationError))
            return Effect().none()
        }

        val event = WorkflowLoaded(runId = runId, definition = cmd.definition)
        return Effect().persist(event).thenRun {
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
        if (cmd.result.status == StepResultStatus.SUCCEEDED) {
            val event = StepCompleted(
                runId = runId,
                stepId = cmd.stepId,
                output = cmd.result.output,
                toolCalls = cmd.result.toolCalls,
                metrics = cmd.result.metrics,
            )
            return Effect().persist(event).thenRun { newState: RunState ->
                onStepSucceeded(newState, cmd.stepId)
            }
        }

        val error = cmd.result.error ?: "Unknown error"
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
            return Effect().persist(retryEvent).thenRun { newState: RunState ->
                scheduleStepExecution(newState, cmd.stepId)
            }
        }

        val membership = findParallelMembership(state, cmd.stepId)
        if (membership != null) {
            val failEvent = StepFailed(
                runId = runId,
                stepId = cmd.stepId,
                error = error,
                retryable = false,
            )
            return Effect().persist(failEvent).thenRun {
                ctx.self.tell(ParallelBranchTerminalInternal(membership.first, membership.second, emptyMap(), error))
            }
        }

        val failEvent = StepFailed(
            runId = runId,
            stepId = cmd.stepId,
            error = error,
            retryable = false,
        )
        val runFailEvent = RunFailed(runId = runId, error = "Step ${cmd.stepId} failed: $error")
        return Effect().persist(listOf(failEvent, runFailEvent)).thenRun {
            stepExecutor.tell(RunTerminated(runId))
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
        cancelChildRuns(state, "Parent run cancelled")
        val event = RunCancelled(runId = runId, reason = cmd.reason)
        return Effect().persist(event).thenRun {
            cmd.replyTo.tell(RunCommandResponse(true, "Run cancelled"))
            stepExecutor.tell(RunTerminated(runId))
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
                parallelGroups = state.parallelGroups.mapValues { (_, value) -> value.copy() },
                subworkflowChildren = state.subworkflowChildren.toMap(),
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
        return Effect().persist(event).thenRun {
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
        return Effect().persist(event).thenRun {
            stepExecutor.tell(RunTerminated(runId))
        }
    }

    private fun onStartParallel(state: RunState, cmd: StartParallelInternal): Effect<RunEvent, RunState> {
        val definition = state.definition ?: return Effect().none()
        val step = definition.steps.find { it.id == cmd.stepId } ?: return Effect().none()
        val joinStepId = step.joinNext
        if (step.parallel.isEmpty() || joinStepId.isNullOrBlank()) {
            val runFail = RunFailed(runId = runId, error = "Parallel step ${step.id} must define parallel branches and join_next")
            val stepFail = StepFailed(runId = runId, stepId = step.id, error = runFail.error, retryable = false)
            return Effect().persist(listOf(stepFail, runFail)).thenRun {
                stepExecutor.tell(RunTerminated(runId))
            }
        }

        val event = ParallelGroupStarted(
            runId = runId,
            parallelStepId = step.id,
            branches = step.parallel,
            joinStepId = joinStepId,
        )
        return Effect().persist(event).thenRun { newState: RunState ->
            step.parallel.forEach { branchStepId ->
                scheduleStepExecution(newState, branchStepId)
            }
        }
    }

    private fun onParallelBranchTerminal(state: RunState, cmd: ParallelBranchTerminalInternal): Effect<RunEvent, RunState> {
        val events = mutableListOf<RunEvent>()
        if (cmd.error != null) {
            events.add(
                ParallelBranchFailed(
                    runId = runId,
                    parallelStepId = cmd.parallelStepId,
                    branchRootStepId = cmd.branchRootStepId,
                    error = cmd.error,
                )
            )
        }
        events.add(
            ParallelBranchCompleted(
                runId = runId,
                parallelStepId = cmd.parallelStepId,
                branchRootStepId = cmd.branchRootStepId,
                branchOutput = cmd.branchOutput,
            )
        )

        return Effect().persist(events).thenRun {
            ctx.self.tell(CheckParallelFanInInternal(cmd.parallelStepId))
        }
    }

    private fun onCheckParallelFanIn(state: RunState, cmd: CheckParallelFanInInternal): Effect<RunEvent, RunState> {
        val group = state.parallelGroups[cmd.parallelStepId] ?: return Effect().none()
        if (group.pendingBranches.isNotEmpty()) {
            return Effect().none()
        }

        if (group.failedBranches.isNotEmpty()) {
            val error = group.failedBranches.entries.joinToString("; ") { (branch, reason) -> "$branch: $reason" }
            val events = listOf(
                ParallelGroupFailed(runId = runId, parallelStepId = cmd.parallelStepId, error = error),
                StepFailed(runId = runId, stepId = cmd.parallelStepId, error = error, retryable = false),
                RunFailed(runId = runId, error = "Parallel step ${cmd.parallelStepId} failed: $error"),
            )
            return Effect().persist(events).thenRun {
                stepExecutor.tell(RunTerminated(runId))
            }
        }

        val parallelOutput = buildParallelOutput(group)
        val successEvent = ParallelGroupCompleted(
            runId = runId,
            parallelStepId = cmd.parallelStepId,
            output = parallelOutput,
        )
        return Effect().persist(successEvent).thenRun { newState: RunState ->
            val definition = newState.definition ?: return@thenRun
            val parallelStep = definition.steps.find { it.id == cmd.parallelStepId } ?: return@thenRun
            val joinNext = parallelStep.joinNext
            if (joinNext != null) {
                scheduleStepExecution(newState, joinNext)
            } else {
                advanceWorkflow(newState, parallelStep.id)
            }
        }
    }

    private fun onStartSubworkflow(state: RunState, cmd: StartSubworkflowInternal): Effect<RunEvent, RunState> {
        val definition = state.definition ?: return Effect().none()
        val step = definition.steps.find { it.id == cmd.stepId } ?: return Effect().none()
        val templateId = step.subworkflow
        val version = step.subworkflowVersion
        if (templateId.isNullOrBlank() || version == null) {
            val error = "Subworkflow step ${step.id} requires subworkflow and subworkflow_version"
            return Effect().persist(
                listOf(
                    StepFailed(runId = runId, stepId = step.id, error = error, retryable = false),
                    RunFailed(runId = runId, error = error),
                )
            ).thenRun {
                stepExecutor.tell(RunTerminated(runId))
            }
        }

        val versionAdapter = ctx.messageAdapter(VersionResponse::class.java) { response ->
            SubworkflowVersionResolvedInternal(step.id, templateId, version, resolveInputExpressions(step.input, state), response)
        }
        registry.tell(GetVersion(templateId, version, versionAdapter))
        return Effect().none()
    }

    private fun onSubworkflowVersionResolved(state: RunState, cmd: SubworkflowVersionResolvedInternal): Effect<RunEvent, RunState> {
        val versionResponse = cmd.response
        val workflowVersion = versionResponse.version
        if (!versionResponse.found || workflowVersion == null) {
            val error = "Subworkflow ${cmd.templateId}:${cmd.versionNumber} not found"
            return Effect().persist(
                listOf(
                    StepFailed(runId = runId, stepId = cmd.stepId, error = error, retryable = false),
                    RunFailed(runId = runId, error = error),
                )
            ).thenRun {
                stepExecutor.tell(RunTerminated(runId))
            }
        }

        val attempt = state.stepAttempts[cmd.stepId] ?: 1
        val childRunId = buildChildRunId(cmd.stepId, attempt)
        val startedEvent = SubworkflowChildStarted(
            runId = runId,
            stepId = cmd.stepId,
            childRunId = childRunId,
            templateId = cmd.templateId,
            versionNumber = cmd.versionNumber,
        )
        return Effect().persist(startedEvent).thenRun {
            startChildRun(childRunId, cmd.templateId, cmd.versionNumber, workflowVersion.definition, cmd.resolvedInput)
            ctx.scheduleOnce(Duration.ofMillis(500), ctx.self, PollSubworkflowStatusInternal(cmd.stepId, childRunId))
        }
    }

    private fun onPollSubworkflowStatus(state: RunState, cmd: PollSubworkflowStatusInternal): Effect<RunEvent, RunState> {
        val child = state.subworkflowChildren[cmd.stepId] ?: return Effect().none()
        if (child.childRunId != cmd.childRunId) {
            return Effect().none()
        }

        val entityRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, cmd.childRunId)
        val statusAdapter = ctx.messageAdapter(RunStatusResponse::class.java) { status ->
            SubworkflowStatusResponseInternal(cmd.stepId, cmd.childRunId, status)
        }
        entityRef.tell(GetRunStatus(statusAdapter))
        return Effect().none()
    }

    private fun onSubworkflowStatusResponse(state: RunState, cmd: SubworkflowStatusResponseInternal): Effect<RunEvent, RunState> {
        val child = state.subworkflowChildren[cmd.stepId] ?: return Effect().none()
        if (child.childRunId != cmd.childRunId) {
            return Effect().none()
        }

        return when (cmd.status.status) {
            org.pekora.dsl.RunState.COMPLETED -> {
                val completeEvent = SubworkflowChildCompleted(
                    runId = runId,
                    stepId = cmd.stepId,
                    childRunId = cmd.childRunId,
                    output = cmd.status.outputs,
                )
                Effect().persist(completeEvent).thenRun {
                    ctx.self.tell(
                        StepResult(
                            stepId = cmd.stepId,
                            result = StepExecutionResult(
                                status = StepResultStatus.SUCCEEDED,
                                output = cmd.status.outputs,
                            )
                        )
                    )
                }
            }
            org.pekora.dsl.RunState.CANCELLED,
            org.pekora.dsl.RunState.FAILED -> {
                val error = cmd.status.error ?: "Child run ${cmd.childRunId} ended with ${cmd.status.status}"
                val failedEvent = SubworkflowChildFailed(
                    runId = runId,
                    stepId = cmd.stepId,
                    childRunId = cmd.childRunId,
                    error = error,
                )
                Effect().persist(failedEvent).thenRun {
                    ctx.self.tell(
                        StepResult(
                            stepId = cmd.stepId,
                            result = StepExecutionResult(
                                status = StepResultStatus.FAILED,
                                error = error,
                            )
                        )
                    )
                }
            }
            else -> {
                ctx.scheduleOnce(Duration.ofMillis(500), ctx.self, PollSubworkflowStatusInternal(cmd.stepId, cmd.childRunId))
                Effect().none()
            }
        }
    }

    private fun onEvaluateDecision(state: RunState, cmd: EvaluateDecisionInternal): Effect<RunEvent, RunState> {
        val definition = state.definition ?: return Effect().none()
        val step = definition.steps.find { it.id == cmd.stepId } ?: return Effect().none()

        val selectedNext = evaluateDecisionNext(step, state)
        if (selectedNext.isNullOrBlank()) {
            val error = "Decision step ${step.id} did not resolve a next step"
            return Effect().persist(
                listOf(
                    StepFailed(runId = runId, stepId = step.id, error = error, retryable = false),
                    RunFailed(runId = runId, error = error),
                )
            ).thenRun {
                stepExecutor.tell(RunTerminated(runId))
            }
        }

        val completeEvent = StepCompleted(
            runId = runId,
            stepId = step.id,
            output = mapOf("selected_next" to selectedNext),
        )
        return Effect().persist(completeEvent).thenRun { newState: RunState ->
            scheduleStepExecution(newState, selectedNext)
        }
    }

    private fun scheduleNextStep(state: RunState) {
        val definition = state.definition ?: return
        val firstStep = definition.steps.firstOrNull() ?: return
        if (state.stepStates[firstStep.id] == null) {
            scheduleStepExecution(state, firstStep.id)
        }
    }

    private fun onStepSucceeded(state: RunState, stepId: String) {
        val membership = findParallelMembership(state, stepId)
        if (membership != null) {
            val definition = state.definition ?: return
            val step = definition.steps.find { it.id == stepId } ?: return
            val group = state.parallelGroups[membership.first] ?: return
            val next = step.next

            if (next == null || next == group.joinStepId) {
                val branchOutput = state.stepOutputs[stepId] ?: emptyMap()
                ctx.self.tell(
                    ParallelBranchTerminalInternal(
                        parallelStepId = membership.first,
                        branchRootStepId = membership.second,
                        branchOutput = branchOutput,
                    )
                )
            } else {
                scheduleStepExecution(state, next)
            }
            return
        }

        advanceWorkflow(state, stepId)
    }

    private fun advanceWorkflow(state: RunState, completedStepId: String) {
        val definition = state.definition ?: return
        val completedStep = definition.steps.find { it.id == completedStepId } ?: return

        val nextStepId = if (completedStep.type == StepKind.DECISION) {
            state.stepOutputs[completedStepId]?.get("selected_next")
        } else {
            completedStep.next
        }

        if (nextStepId == null) {
            if (completedStep.type == StepKind.RESULT) {
                val resultOutput = state.stepOutputs[completedStepId] ?: emptyMap()
                ctx.self.tell(CompleteRunInternal(resultOutput))
            }
            return
        }

        scheduleStepExecution(state, nextStepId)
    }

    private fun scheduleStepExecution(state: RunState, stepId: String) {
        val definition = state.definition ?: return
        val step = definition.steps.find { it.id == stepId } ?: return

        when (step.type) {
            StepKind.APPROVAL -> {
                val approvalId = "approval_${runId}_${stepId}"
                ctx.self.tell(RequestApprovalInternal(stepId, approvalId, step.approvers))
            }
            StepKind.RESULT -> {
                val resolvedOutput = resolveInputExpressions(step.output, state)
                stepExecutor.tell(ExecuteResultStep(runId, stepId, resolvedOutput, ctx.self))
            }
            StepKind.PARALLEL -> {
                ctx.self.tell(StartParallelInternal(stepId))
            }
            StepKind.SUBWORKFLOW -> {
                ctx.self.tell(StartSubworkflowInternal(stepId))
            }
            StepKind.DECISION -> {
                ctx.self.tell(EvaluateDecisionInternal(stepId))
            }
            else -> {
                val resolvedInput = resolveInputExpressions(step.input, state)
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
                stepExecutor.tell(
                    ExecuteStep(
                        request = request,
                        replyTo = ctx.self,
                        stepDefinition = step,
                        agents = agents,
                        stepPolicies = workflowPolicies,
                    )
                )
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
            parts[0] == "inputs" && parts.size >= 2 -> state.inputs[parts[1]] ?: ""
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

    private fun startChildRun(
        childRunId: String,
        templateId: String,
        versionNumber: Int,
        definition: WorkflowDefinition,
        inputs: Map<String, String>,
    ) {
        val childRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, childRunId)
        childRef.tell(
            CreateRun(
                templateId = templateId,
                versionNumber = versionNumber,
                inputs = inputs,
                tenantId = "",
                correlationId = "${runId}:${childRunId}",
                replyTo = childCommandAckAdapter,
            )
        )
        childRef.tell(LoadWorkflow(definition, childCommandAckAdapter))
        childRef.tell(StartRun(childCommandAckAdapter))
    }

    private fun cancelChildRuns(state: RunState, reason: String) {
        state.subworkflowChildren.values.forEach { child ->
            if (child.status in listOf(
                    org.pekora.dsl.RunState.CREATED,
                    org.pekora.dsl.RunState.LOADING_DEFINITION,
                    org.pekora.dsl.RunState.READY,
                    org.pekora.dsl.RunState.EXECUTING,
                    org.pekora.dsl.RunState.WAITING_FOR_APPROVAL,
                    org.pekora.dsl.RunState.WAITING_FOR_EXTERNAL_EVENT,
                )
            ) {
                val childRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, child.childRunId)
                childRef.tell(CancelRun(reason = reason, replyTo = childCommandAckAdapter))
            }
        }
    }

    private fun buildParallelOutput(group: ParallelGroupState): Map<String, String> {
        val output = mutableMapOf<String, String>()
        group.branchOutputs.forEach { (branchId, branchOutput) ->
            if (branchOutput.isEmpty()) {
                output["$branchId.__empty"] = "true"
            } else {
                branchOutput.forEach { (key, value) ->
                    output["$branchId.$key"] = value
                }
            }
        }
        return output
    }

    private fun findParallelMembership(state: RunState, stepId: String): Pair<String, String>? {
        val definition = state.definition ?: return null
        state.parallelGroups.forEach { (parallelStepId, group) ->
            group.branchRoots.forEach { branchRoot ->
                if (isStepReachable(definition, branchRoot, stepId, group.joinStepId, mutableSetOf())) {
                    return parallelStepId to branchRoot
                }
            }
        }
        return null
    }

    private fun isStepReachable(
        definition: WorkflowDefinition,
        currentStepId: String,
        targetStepId: String,
        stopAtStepId: String,
        visited: MutableSet<String>,
    ): Boolean {
        if (!visited.add(currentStepId)) {
            return false
        }
        if (currentStepId == targetStepId) {
            return true
        }
        if (currentStepId == stopAtStepId) {
            return false
        }

        val step = definition.steps.find { it.id == currentStepId } ?: return false
        if (step.type == StepKind.DECISION) {
            return step.branches.any { branch ->
                isStepReachable(definition, branch.next, targetStepId, stopAtStepId, visited.toMutableSet())
            }
        }

        val next = step.next ?: return false
        return isStepReachable(definition, next, targetStepId, stopAtStepId, visited)
    }

    private fun evaluateDecisionNext(step: StepDefinition, state: RunState): String? {
        val conditionPattern = Regex("""^([A-Za-z0-9_.]+)\s*(==|!=)\s*'([^']*)'$""")

        for (branch in step.branches) {
            val condition = branch.condition.trim()
            val matches = conditionPattern.matchEntire(condition)
            val passed = when {
                condition.equals("true", ignoreCase = true) -> true
                condition.equals("false", ignoreCase = true) -> false
                matches != null -> {
                    val left = resolveExpression(matches.groupValues[1], state)
                    val op = matches.groupValues[2]
                    val right = matches.groupValues[3]
                    if (op == "==") left == right else left != right
                }
                else -> false
            }
            if (passed) {
                return branch.next
            }
        }

        return step.next
    }

    private fun validateWorkflow(definition: WorkflowDefinition): String? {
        val stepsById = definition.steps.associateBy { it.id }

        definition.steps.forEach { step ->
            if (step.type == StepKind.SUBWORKFLOW) {
                if (step.subworkflow.isNullOrBlank()) {
                    return "Subworkflow step ${step.id} must define subworkflow"
                }
                val subworkflowVersion = step.subworkflowVersion
                if (subworkflowVersion == null || subworkflowVersion <= 0) {
                    return "Subworkflow step ${step.id} must define subworkflow_version > 0"
                }
            }

            if (step.type == StepKind.PARALLEL) {
                if (step.parallel.isEmpty()) {
                    return "Parallel step ${step.id} must define at least one branch"
                }
                val joinNext = step.joinNext
                if (joinNext.isNullOrBlank()) {
                    return "Parallel step ${step.id} must define join_next"
                }
                if (!stepsById.containsKey(joinNext)) {
                    return "Parallel step ${step.id} references unknown join_next: $joinNext"
                }
                step.parallel.forEach { branchRoot ->
                    if (!stepsById.containsKey(branchRoot)) {
                        return "Parallel step ${step.id} references unknown branch root: $branchRoot"
                    }
                }

                val ownerByStep = mutableMapOf<String, String>()
                val allowedBranchKinds = setOf(StepKind.AGENT, StepKind.DECISION, StepKind.APPROVAL, StepKind.WAIT)
                for (branchRoot in step.parallel) {
                    val error = validateParallelBranch(
                        definition = definition,
                        currentStepId = branchRoot,
                        joinStepId = joinNext,
                        branchRoot = branchRoot,
                        ownerByStep = ownerByStep,
                        allowedBranchKinds = allowedBranchKinds,
                        visited = mutableSetOf(),
                    )
                    if (error != null) {
                        return error
                    }
                }
            }
        }

        return null
    }

    private fun validateParallelBranch(
        definition: WorkflowDefinition,
        currentStepId: String,
        joinStepId: String,
        branchRoot: String,
        ownerByStep: MutableMap<String, String>,
        allowedBranchKinds: Set<StepKind>,
        visited: MutableSet<String>,
    ): String? {
        if (currentStepId == joinStepId) {
            return null
        }
        if (!visited.add(currentStepId)) {
            return "Parallel branch $branchRoot contains a cycle at step $currentStepId"
        }

        val step = definition.steps.find { it.id == currentStepId }
            ?: return "Parallel branch $branchRoot references unknown step $currentStepId"

        val owner = ownerByStep[currentStepId]
        if (owner != null && owner != branchRoot) {
            return "Parallel branches must be disjoint until join_next; step $currentStepId is used by both $owner and $branchRoot"
        }
        ownerByStep[currentStepId] = branchRoot

        if (step.type !in allowedBranchKinds) {
            return "Parallel branch step $currentStepId has unsupported type ${step.type}"
        }

        if (step.type == StepKind.DECISION) {
            step.branches.forEach { branch ->
                val branchError = validateParallelBranch(
                    definition = definition,
                    currentStepId = branch.next,
                    joinStepId = joinStepId,
                    branchRoot = branchRoot,
                    ownerByStep = ownerByStep,
                    allowedBranchKinds = allowedBranchKinds,
                    visited = visited.toMutableSet(),
                )
                if (branchError != null) {
                    return branchError
                }
            }
            return null
        }

        val next = step.next ?: return null
        return validateParallelBranch(
            definition = definition,
            currentStepId = next,
            joinStepId = joinStepId,
            branchRoot = branchRoot,
            ownerByStep = ownerByStep,
            allowedBranchKinds = allowedBranchKinds,
            visited = visited,
        )
    }

    private fun buildChildRunId(stepId: String, attempt: Int): String = "${runId}__${stepId}__${attempt}"
}

internal data class CompleteRunInternal(val outputs: Map<String, String>) : RunCommand

internal data class RequestApprovalInternal(
    val stepId: String,
    val approvalId: String,
    val approvers: List<String>,
) : RunCommand

internal data class StartParallelInternal(val stepId: String) : RunCommand

internal data class ParallelBranchTerminalInternal(
    val parallelStepId: String,
    val branchRootStepId: String,
    val branchOutput: Map<String, String>,
    val error: String? = null,
) : RunCommand

internal data class CheckParallelFanInInternal(val parallelStepId: String) : RunCommand

internal data class StartSubworkflowInternal(val stepId: String) : RunCommand

internal data class SubworkflowVersionResolvedInternal(
    val stepId: String,
    val templateId: String,
    val versionNumber: Int,
    val resolvedInput: Map<String, String>,
    val response: VersionResponse,
) : RunCommand

internal data class PollSubworkflowStatusInternal(
    val stepId: String,
    val childRunId: String,
) : RunCommand

internal data class SubworkflowStatusResponseInternal(
    val stepId: String,
    val childRunId: String,
    val status: RunStatusResponse,
) : RunCommand

internal data class EvaluateDecisionInternal(val stepId: String) : RunCommand

internal data object IgnoreChildCommandAckInternal : RunCommand
