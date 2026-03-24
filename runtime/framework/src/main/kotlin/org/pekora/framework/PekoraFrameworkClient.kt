package org.pekora.framework

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.pekora.engine.ApprovalManagerResponse
import org.pekora.engine.GetPendingApprovals
import org.pekora.engine.GetRunStatus
import org.pekora.engine.PendingApprovalsResponse
import org.pekora.engine.ResumeRun
import org.pekora.engine.RunCommandResponse
import org.pekora.engine.RunEntityTypeKey
import org.pekora.engine.RunStatusResponse
import org.pekora.engine.SubmitApproval
import org.pekora.engine.CancelRun
import org.pekora.engine.CreateRun
import org.pekora.engine.LoadWorkflow
import org.pekora.engine.StartRun
import org.pekora.projection.RunSummary
import org.pekora.projection.RunTimeline
import org.pekora.registry.GetLatestVersion
import org.pekora.registry.GetVersion
import org.pekora.registry.ListTemplates
import org.pekora.registry.PublishVersion
import org.pekora.registry.RegisterTemplate
import org.pekora.registry.RegistryResponse
import org.pekora.registry.TemplateListResponse
import org.pekora.registry.VersionResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

data class CreateRunSpec(
    val runId: String = "",
    val templateId: String,
    val version: Int = 0,
    val inputs: Map<String, String> = emptyMap(),
    val tenantId: String = "",
    val correlationId: String = "",
)

data class CreateRunResult(
    val runId: String,
    val message: String,
)

data class StepOutput(
    val runId: String,
    val stepId: String,
    val output: Map<String, String>,
)

class PekoraFrameworkClient(
    private val runtime: PekoraFrameworkRuntime,
    private val askTimeout: Duration = Duration.ofSeconds(10),
) {
    fun createTemplate(
        id: String,
        name: String,
        description: String = "",
        owner: String = "",
        tenantId: String = "",
    ): CompletionStage<RegistryResponse> = AskPattern.ask(
        runtime.registry,
        { replyTo: ActorRef<RegistryResponse> ->
            RegisterTemplate(id, name, description, owner, tenantId, replyTo)
        },
        askTimeout,
        runtime.system.scheduler(),
    )

    fun publishVersion(
        templateId: String,
        version: Int,
        definition: org.pekora.dsl.WorkflowDefinition,
    ): CompletionStage<RegistryResponse> = AskPattern.ask(
        runtime.registry,
        { replyTo: ActorRef<RegistryResponse> ->
            PublishVersion(templateId, version, definition, replyTo)
        },
        askTimeout,
        runtime.system.scheduler(),
    )

    fun listTemplates(tenantId: String = ""): CompletionStage<TemplateListResponse> = AskPattern.ask(
        runtime.registry,
        { replyTo: ActorRef<TemplateListResponse> -> ListTemplates(tenantId, replyTo) },
        askTimeout,
        runtime.system.scheduler(),
    )

    fun createRun(spec: CreateRunSpec): CompletionStage<CreateRunResult> {
        val runId = spec.runId.ifEmpty { "run_${UUID.randomUUID()}" }
        val entityRef = runtime.sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)

        val versionFuture = AskPattern.ask(
            runtime.registry,
            { replyTo: ActorRef<VersionResponse> ->
                if (spec.version > 0) {
                    GetVersion(spec.templateId, spec.version, replyTo)
                } else {
                    GetLatestVersion(spec.templateId, replyTo)
                }
            },
            askTimeout,
            runtime.system.scheduler(),
        )

        return versionFuture.thenCompose { versionResp ->
            val resolvedVersion = versionResp.version
            if (!versionResp.found || resolvedVersion == null) {
                CompletableFuture.failedFuture(IllegalArgumentException("Workflow version not found"))
            } else {
                AskPattern.ask(
                    entityRef,
                    { replyTo: ActorRef<RunCommandResponse> ->
                        CreateRun(
                            templateId = spec.templateId,
                            versionNumber = resolvedVersion.version,
                            inputs = spec.inputs,
                            tenantId = spec.tenantId,
                            correlationId = spec.correlationId,
                            replyTo = replyTo,
                        )
                    },
                    askTimeout,
                    runtime.system.scheduler(),
                ).thenCompose { createResp ->
                    if (!createResp.success) {
                        CompletableFuture.failedFuture(IllegalStateException(createResp.message))
                    } else {
                        AskPattern.ask(
                            entityRef,
                            { replyTo: ActorRef<RunCommandResponse> ->
                                LoadWorkflow(resolvedVersion.definition, replyTo)
                            },
                            askTimeout,
                            runtime.system.scheduler(),
                        ).thenCompose {
                            AskPattern.ask(
                                entityRef,
                                { replyTo: ActorRef<RunCommandResponse> -> StartRun(replyTo) },
                                askTimeout,
                                runtime.system.scheduler(),
                            ).thenApply { startResp ->
                                CreateRunResult(runId, startResp.message)
                            }
                        }
                    }
                }
            }
        }
    }

    fun getRunStatus(runId: String): CompletionStage<RunStatusResponse> = AskPattern.ask(
        runtime.sharding.entityRefFor(RunEntityTypeKey.typeKey, runId),
        { replyTo: ActorRef<RunStatusResponse> -> GetRunStatus(replyTo) },
        askTimeout,
        runtime.system.scheduler(),
    )

    fun cancelRun(runId: String, reason: String = "Programmatic cancel request"): CompletionStage<RunCommandResponse> = AskPattern.ask(
        runtime.sharding.entityRefFor(RunEntityTypeKey.typeKey, runId),
        { replyTo: ActorRef<RunCommandResponse> -> CancelRun(reason, replyTo) },
        askTimeout,
        runtime.system.scheduler(),
    )

    fun resumeRun(runId: String): CompletionStage<RunCommandResponse> = AskPattern.ask(
        runtime.sharding.entityRefFor(RunEntityTypeKey.typeKey, runId),
        { replyTo: ActorRef<RunCommandResponse> -> ResumeRun(replyTo) },
        askTimeout,
        runtime.system.scheduler(),
    )

    fun listRuns(tenantId: String? = null): List<RunSummary> = runtime.runProjection.listRuns(tenantId)

    fun listActiveRuns(tenantId: String? = null): List<RunSummary> =
        runtime.runProjection.getActiveRuns().filter { summary ->
            tenantId == null || summary.tenantId == tenantId
        }

    fun getRunTimeline(runId: String): RunTimeline = runtime.runProjection.getTimeline(runId)

    fun getStepOutput(runId: String, stepId: String): StepOutput? {
        val output = runtime.runProjection.getStepOutput(runId, stepId) ?: return null
        return StepOutput(runId, stepId, output)
    }

    fun getPendingApprovals(): CompletionStage<PendingApprovalsResponse> = AskPattern.ask(
        runtime.approvalManager,
        { replyTo: ActorRef<PendingApprovalsResponse> -> GetPendingApprovals(replyTo) },
        askTimeout,
        runtime.system.scheduler(),
    )

    fun approve(
        approvalId: String,
        approver: String = "",
        reason: String = "",
    ): CompletionStage<ApprovalManagerResponse> = submitApproval(approvalId, true, approver, reason)

    fun reject(
        approvalId: String,
        approver: String = "",
        reason: String = "",
    ): CompletionStage<ApprovalManagerResponse> = submitApproval(approvalId, false, approver, reason)

    private fun submitApproval(
        approvalId: String,
        approved: Boolean,
        approver: String,
        reason: String,
    ): CompletionStage<ApprovalManagerResponse> = AskPattern.ask(
        runtime.approvalManager,
        { replyTo: ActorRef<ApprovalManagerResponse> ->
            SubmitApproval(approvalId, approved, approver, reason, replyTo)
        },
        askTimeout,
        runtime.system.scheduler(),
    )
}
