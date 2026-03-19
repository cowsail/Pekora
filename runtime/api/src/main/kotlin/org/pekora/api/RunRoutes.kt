/**
 * HTTP route definitions for workflow run lifecycle and approval management
 * (Section 15.2 and Section 15.3).
 *
 * This file defines the Runtime API surface of the Pekko Agent Workflow Framework.
 * It exposes RESTful endpoints for creating, querying, cancelling, and resuming
 * workflow runs, as well as for managing human-in-the-loop approval gates.
 *
 * **Run lifecycle orchestration** (Section 15.2): Creating a run follows a three-phase
 * sequence within a single HTTP request:
 *
 * 1. **Create** -- a [CreateRun] command initializes the run entity with metadata and inputs.
 * 2. **Load** -- a [LoadWorkflow] command supplies the resolved [WorkflowDefinition] to
 *    the entity so it knows which steps to execute.
 * 3. **Start** -- a [StartRun] command transitions the run to EXECUTING and begins
 *    step processing.
 *
 * **Approval routes** (Section 15.3): When a step's policy requires human approval, the
 * run pauses in WAITING_FOR_APPROVAL state. The approval endpoints allow external
 * reviewers to approve or reject pending gates, which resumes the run.
 *
 * **Endpoints provided:**
 *
 * | Method | Path                             | Description                      |
 * |--------|----------------------------------|----------------------------------|
 * | POST   | `/runs`                          | Create and start a new run       |
 * | GET    | `/runs/{runId}`                  | Get current run status           |
 * | POST   | `/runs/{runId}/cancel`           | Cancel a running workflow        |
 * | POST   | `/runs/{runId}/resume`           | Resume a paused run              |
 * | GET    | `/approvals`                     | List pending approval gates      |
 * | POST   | `/approvals/{approvalId}/approve`| Approve a pending gate           |
 * | POST   | `/approvals/{approvalId}/reject` | Reject a pending gate            |
 *
 * @see RunRoutes
 * @see WorkflowRoutes
 */
package org.pekora.api

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding
import org.apache.pekko.http.javadsl.marshallers.jackson.Jackson
import org.apache.pekko.http.javadsl.model.StatusCodes
import org.apache.pekko.http.javadsl.server.AllDirectives
import org.apache.pekko.http.javadsl.server.PathMatchers
import org.apache.pekko.http.javadsl.server.Route
import org.pekora.engine.*
import org.pekora.registry.*
import java.time.Duration
import java.util.UUID

/**
 * Pekko HTTP route handler for the Runtime API (Section 15.2) and Approval API
 * (Section 15.3).
 *
 * Manages the full run lifecycle -- creation, status queries, cancellation, and
 * resumption -- as well as the human-in-the-loop approval workflow. Run entities are
 * accessed via [ClusterSharding], and the workflow registry is consulted to resolve
 * version metadata before a run is created.
 *
 * @property sharding The Pekko [ClusterSharding] extension used to obtain entity
 *   references to [RunEntity] instances.
 * @property registry An [ActorRef] to the workflow registry actor, used to resolve
 *   template versions when creating runs.
 * @property approvalManager An [ActorRef] to the [ApprovalManager] actor that tracks
 *   pending approval gates.
 * @property system The Pekko [ActorSystem] used to obtain the scheduler for ask-pattern calls.
 * @see WorkflowRoutes
 * @see org.pekora.engine.RunEntity
 */
class RunRoutes(
    private val sharding: ClusterSharding,
    private val registry: ActorRef<RegistryCommand>,
    private val approvalManager: ActorRef<ApprovalCommand>,
    private val system: ActorSystem<*>,
) : AllDirectives() {

    private val askTimeout = Duration.ofSeconds(10)

    /**
     * Builds and returns the composite [Route] for all run and approval endpoints.
     *
     * @return A Pekko HTTP [Route] combining [runsRoutes] and [approvalsRoutes].
     * @see runsRoutes
     * @see approvalsRoutes
     */
    fun routes(): Route = concat(
        runsRoutes(),
        approvalsRoutes(),
    )

    private fun runsRoutes(): Route = pathPrefix("runs") {
        concat(
            // POST /runs — create and start a new run
            pathEnd {
                post {
                    entity(Jackson.unmarshaller(CreateRunRequest::class.java)) { req ->
                        val runId = req.runId.ifEmpty { "run_${UUID.randomUUID()}" }
                        val entityRef = sharding.entityRefFor(
                            RunEntityTypeKey.typeKey,
                            runId,
                        )

                        // First get the workflow version from registry
                        val versionFuture = AskPattern.ask(
                            registry,
                            { replyTo: ActorRef<VersionResponse> ->
                                if (req.version > 0) {
                                    GetVersion(req.templateId, req.version, replyTo)
                                } else {
                                    GetLatestVersion(req.templateId, replyTo)
                                }
                            },
                            askTimeout,
                            system.scheduler(),
                        )

                        onSuccess(versionFuture) { versionResp ->
                            if (!versionResp.found || versionResp.version == null) {
                                complete(StatusCodes.NOT_FOUND, "Workflow version not found")
                            } else {
                                val wv = versionResp.version!!

                                // Create the run
                                val createFuture = AskPattern.ask(
                                    entityRef,
                                    { replyTo: ActorRef<RunCommandResponse> ->
                                        CreateRun(
                                            templateId = req.templateId,
                                            versionNumber = wv.version,
                                            inputs = req.inputs,
                                            tenantId = req.tenantId,
                                            correlationId = req.correlationId,
                                            replyTo = replyTo,
                                        )
                                    },
                                    askTimeout,
                                    system.scheduler(),
                                )

                                onSuccess(createFuture) { createResp ->
                                    if (!createResp.success) {
                                        complete(StatusCodes.BAD_REQUEST, createResp, Jackson.marshaller())
                                    } else {
                                        // Load workflow
                                        val loadFuture = AskPattern.ask(
                                            entityRef,
                                            { replyTo: ActorRef<RunCommandResponse> ->
                                                LoadWorkflow(wv.definition, replyTo)
                                            },
                                            askTimeout,
                                            system.scheduler(),
                                        )

                                        onSuccess(loadFuture) { _ ->
                                            // Start the run
                                            val startFuture = AskPattern.ask(
                                                entityRef,
                                                { replyTo: ActorRef<RunCommandResponse> ->
                                                    StartRun(replyTo)
                                                },
                                                askTimeout,
                                                system.scheduler(),
                                            )

                                            onSuccess(startFuture) { startResp ->
                                                complete(
                                                    StatusCodes.CREATED,
                                                    CreateRunResponse(runId, startResp.message),
                                                    Jackson.marshaller(),
                                                )
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            },
            // GET /runs/{runId} — get run status
            path(PathMatchers.segment()) { runId ->
                get {
                    val entityRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
                    val future = AskPattern.ask(
                        entityRef,
                        { replyTo: ActorRef<RunStatusResponse> ->
                            GetRunStatus(replyTo)
                        },
                        askTimeout,
                        system.scheduler(),
                    )
                    onSuccess(future) { response ->
                        complete(StatusCodes.OK, response, Jackson.marshaller())
                    }
                }
            },
            // POST /runs/{runId}/cancel
            path(PathMatchers.segment().slash("cancel")) { runId ->
                post {
                    val entityRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
                    val future = AskPattern.ask(
                        entityRef,
                        { replyTo: ActorRef<RunCommandResponse> ->
                            CancelRun(reason = "API cancel request", replyTo = replyTo)
                        },
                        askTimeout,
                        system.scheduler(),
                    )
                    onSuccess(future) { response ->
                        complete(StatusCodes.OK, response, Jackson.marshaller())
                    }
                }
            },
            // POST /runs/{runId}/resume
            path(PathMatchers.segment().slash("resume")) { runId ->
                post {
                    val entityRef = sharding.entityRefFor(RunEntityTypeKey.typeKey, runId)
                    val future = AskPattern.ask(
                        entityRef,
                        { replyTo: ActorRef<RunCommandResponse> ->
                            ResumeRun(replyTo)
                        },
                        askTimeout,
                        system.scheduler(),
                    )
                    onSuccess(future) { response ->
                        complete(StatusCodes.OK, response, Jackson.marshaller())
                    }
                }
            },
        )
    }

    /**
     * Approval API routes (Section 15.3).
     *
     * Provides endpoints for listing pending approval gates and submitting
     * approve/reject decisions. These routes interact with the [ApprovalManager]
     * actor to coordinate human-in-the-loop approval workflows.
     */
    private fun approvalsRoutes(): Route = pathPrefix("approvals") {
        concat(
            // GET /approvals — list pending
            pathEnd {
                get {
                    val future = AskPattern.ask(
                        approvalManager,
                        { replyTo: ActorRef<PendingApprovalsResponse> ->
                            GetPendingApprovals(replyTo)
                        },
                        askTimeout,
                        system.scheduler(),
                    )
                    onSuccess(future) { response ->
                        complete(StatusCodes.OK, response, Jackson.marshaller())
                    }
                }
            },
            // POST /approvals/{approvalId}/approve
            path(PathMatchers.segment().slash("approve")) { approvalId ->
                post {
                    entity(Jackson.unmarshaller(ApprovalRequest::class.java)) { req ->
                        val future = AskPattern.ask(
                            approvalManager,
                            { replyTo: ActorRef<ApprovalManagerResponse> ->
                                SubmitApproval(
                                    approvalId = approvalId,
                                    approved = true,
                                    approver = req.approver,
                                    reason = req.reason,
                                    replyTo = replyTo,
                                )
                            },
                            askTimeout,
                            system.scheduler(),
                        )
                        onSuccess(future) { response ->
                            if (response.success) {
                                complete(StatusCodes.OK, response, Jackson.marshaller())
                            } else {
                                complete(StatusCodes.NOT_FOUND, response, Jackson.marshaller())
                            }
                        }
                    }
                }
            },
            // POST /approvals/{approvalId}/reject
            path(PathMatchers.segment().slash("reject")) { approvalId ->
                post {
                    entity(Jackson.unmarshaller(ApprovalRequest::class.java)) { req ->
                        val future = AskPattern.ask(
                            approvalManager,
                            { replyTo: ActorRef<ApprovalManagerResponse> ->
                                SubmitApproval(
                                    approvalId = approvalId,
                                    approved = false,
                                    approver = req.approver,
                                    reason = req.reason,
                                    replyTo = replyTo,
                                )
                            },
                            askTimeout,
                            system.scheduler(),
                        )
                        onSuccess(future) { response ->
                            complete(StatusCodes.OK, response, Jackson.marshaller())
                        }
                    }
                }
            },
        )
    }
}

// --- Request/Response DTOs ---

/**
 * Request body for `POST /runs` to create and start a new workflow run.
 *
 * The run lifecycle is orchestrated as a create-load-start sequence within a single
 * HTTP request. If [version] is `0` (the default), the latest published version of
 * the template is used.
 *
 * @property runId Optional client-supplied run identifier. When empty, a UUID-based
 *   identifier is generated automatically.
 * @property templateId The workflow template to instantiate.
 * @property version The specific version number to use, or `0` for the latest version.
 * @property inputs A map of input key-value pairs to pass to the workflow definition.
 * @property tenantId Optional tenant identifier for multi-tenant deployments.
 * @property correlationId Optional external correlation identifier for tracing across
 *   systems.
 * @see RunRoutes
 * @see CreateRunResponse
 */
data class CreateRunRequest(
    val runId: String = "",
    val templateId: String = "",
    val version: Int = 0,
    val inputs: Map<String, String> = emptyMap(),
    val tenantId: String = "",
    val correlationId: String = "",
)

/**
 * Response body returned by `POST /runs` after a run has been successfully created
 * and started.
 *
 * @property runId The unique identifier assigned to the new run.
 * @property message A human-readable status message from the start operation.
 * @see CreateRunRequest
 */
data class CreateRunResponse(
    val runId: String,
    val message: String,
)

/**
 * Request body for `POST /approvals/{approvalId}/approve` and
 * `POST /approvals/{approvalId}/reject`.
 *
 * @property approver The identity of the person or system submitting the approval decision.
 * @property reason An optional reason or justification for the decision.
 * @see RunRoutes
 */
data class ApprovalRequest(
    val approver: String = "",
    val reason: String = "",
)
