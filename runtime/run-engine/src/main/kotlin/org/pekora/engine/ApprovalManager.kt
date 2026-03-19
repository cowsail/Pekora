/**
 * # ApprovalManager — Human-in-the-Loop Approval Gate Actor
 *
 * This file defines the [ApprovalManager] actor and its message protocol, implementing the
 * approval workflow described in Section 6.2 of the architecture. The `ApprovalManager`
 * serves as a centralized coordinator for human approval gates within workflow runs.
 *
 * ## Approval Lifecycle
 *
 * 1. **Request**: When a [RunEntity] encounters an APPROVAL step, it sends a [RequestApproval]
 *    command to the `ApprovalManager`. The manager registers the approval as pending, recording
 *    the approval ID, run ID, step ID, authorized approvers, and the originating `RunEntity`
 *    actor reference.
 *
 * 2. **Pending**: While pending, the approval can be discovered via [GetPendingApprovals],
 *    which returns a list of [PendingApprovalInfo] records. External systems (e.g., a UI or
 *    API gateway) use this to present approval requests to authorized users.
 *
 * 3. **Resolution**: An authorized approver submits a decision via [SubmitApproval]. The
 *    manager removes the approval from the pending map and forwards an [ApprovalResponse]
 *    command to the originating `RunEntity`, which either advances the workflow (if approved)
 *    or marks the step as cancelled (if rejected).
 *
 * ## Pending Approvals Tracking
 *
 * The manager maintains an in-memory [MutableMap] of pending approvals keyed by approval ID.
 * Each entry stores the full [PendingApproval] record including the [RunEntity] actor reference
 * needed to deliver the decision. This map is not persisted; if the manager restarts, pending
 * approvals are lost and must be re-requested by the recovering `RunEntity` instances.
 *
 * @see RunEntity
 * @see ApprovalCommand
 */
package org.pekora.engine

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import org.slf4j.LoggerFactory

/**
 * Sealed interface for all commands accepted by the [ApprovalManager] actor.
 *
 * @see RequestApproval
 * @see SubmitApproval
 * @see GetPendingApprovals
 */
sealed interface ApprovalCommand

/**
 * Command to register a new approval request with the [ApprovalManager].
 *
 * Sent by [RunEntity] when a workflow step of type [APPROVAL][StepKind.APPROVAL] is encountered.
 * The manager records this as a pending approval and waits for a corresponding [SubmitApproval].
 *
 * @property approvalId A unique identifier for this approval request (typically `"approval_{runId}_{stepId}"`).
 * @property runId The workflow run identifier that originated this approval request.
 * @property stepId The identifier of the APPROVAL step in the workflow definition.
 * @property approvers The list of approver identifiers authorized to grant or deny this request.
 * @property runEntity The [RunEntity] actor reference to which the approval decision will be forwarded.
 *
 * @see ApprovalManager
 * @see ApprovalResponse
 */
data class RequestApproval(
    val approvalId: String,
    val runId: String,
    val stepId: String,
    val approvers: List<String>,
    val runEntity: ActorRef<RunCommand>,
) : ApprovalCommand

/**
 * Command to submit an approval decision for a pending approval request.
 *
 * Sent by external systems (e.g., REST API, UI) when an authorized approver grants or
 * denies a pending approval. The manager looks up the pending approval by [approvalId],
 * forwards the decision to the originating [RunEntity], and replies to the caller with
 * an [ApprovalManagerResponse].
 *
 * @property approvalId The unique identifier of the approval request being responded to.
 * @property approved `true` if the approval is granted, `false` if rejected.
 * @property approver The identity of the person or system submitting the decision. Defaults to empty string.
 * @property reason An optional explanation for the approval or rejection. Defaults to empty string.
 * @property replyTo The actor reference to receive the [ApprovalManagerResponse] acknowledgement.
 *
 * @see ApprovalManager
 */
data class SubmitApproval(
    val approvalId: String,
    val approved: Boolean,
    val approver: String = "",
    val reason: String = "",
    val replyTo: ActorRef<ApprovalManagerResponse>,
) : ApprovalCommand

/**
 * Query command to retrieve all currently pending approval requests.
 *
 * This is a read-only operation. The manager replies with a [PendingApprovalsResponse]
 * containing a snapshot of all pending approvals.
 *
 * @property replyTo The actor reference to receive the [PendingApprovalsResponse].
 *
 * @see PendingApprovalsResponse
 */
data class GetPendingApprovals(
    val replyTo: ActorRef<PendingApprovalsResponse>,
) : ApprovalCommand

// --- Responses ---

/**
 * Standard acknowledgement response for [SubmitApproval] commands.
 *
 * @property success `true` if the approval was found and the decision was forwarded;
 *                   `false` if the approval ID was not found in the pending map.
 * @property message A human-readable description of the outcome. Defaults to empty string.
 */
data class ApprovalManagerResponse(
    val success: Boolean,
    val message: String = "",
)

/**
 * Response to [GetPendingApprovals], containing a snapshot of all currently pending approval requests.
 *
 * @property approvals The list of [PendingApprovalInfo] records representing each pending approval.
 *
 * @see GetPendingApprovals
 */
data class PendingApprovalsResponse(
    val approvals: List<PendingApprovalInfo>,
)

/**
 * Data transfer object representing a single pending approval request, returned by [GetPendingApprovals].
 *
 * This is a read-only projection of the internal [PendingApproval] state, excluding the
 * [RunEntity] actor reference for safety (external callers should not have direct access
 * to entity references).
 *
 * @property approvalId The unique identifier of the approval request.
 * @property runId The workflow run identifier that originated this approval.
 * @property stepId The identifier of the APPROVAL step in the workflow definition.
 * @property approvers The list of approver identifiers authorized to act on this request.
 * @property requestedAt The epoch millisecond timestamp when the approval was first requested.
 */
data class PendingApprovalInfo(
    val approvalId: String,
    val runId: String,
    val stepId: String,
    val approvers: List<String>,
    val requestedAt: Long,
)

/**
 * The centralized approval manager actor that tracks pending approval requests and routes
 * approval decisions to the originating [RunEntity] actors.
 *
 * This actor implements the human-in-the-loop approval gate pattern described in Section 6.2.
 * It maintains an in-memory map of pending approvals and provides commands for registering
 * new requests ([RequestApproval]), submitting decisions ([SubmitApproval]), and querying
 * pending approvals ([GetPendingApprovals]).
 *
 * The pending approvals map is not persisted. If the `ApprovalManager` actor restarts,
 * pending approvals are lost and must be re-requested by the recovering [RunEntity] instances.
 *
 * @param context The typed actor context for this behavior.
 *
 * @see ApprovalCommand
 * @see RunEntity
 */
class ApprovalManager(
    context: ActorContext<ApprovalCommand>,
) : AbstractBehavior<ApprovalCommand>(context) {

    /**
     * Companion object providing the factory method and logger for [ApprovalManager].
     */
    companion object {
        private val logger = LoggerFactory.getLogger(ApprovalManager::class.java)

        /**
         * Factory method that creates a new [ApprovalManager] behavior.
         *
         * @return A [Behavior] that, when materialized, produces a fully initialized [ApprovalManager].
         */
        fun create(): Behavior<ApprovalCommand> = Behaviors.setup { ctx ->
            ApprovalManager(ctx)
        }
    }

    private data class PendingApproval(
        val approvalId: String,
        val runId: String,
        val stepId: String,
        val approvers: List<String>,
        val runEntity: ActorRef<RunCommand>,
        val requestedAt: Long = System.currentTimeMillis(),
    )

    private val pending = mutableMapOf<String, PendingApproval>()

    /**
     * Constructs the [Receive] handler that routes incoming [ApprovalCommand] instances
     * to the appropriate handler method.
     *
     * @return A [Receive] instance handling [RequestApproval], [SubmitApproval], and
     *         [GetPendingApprovals] messages.
     */
    override fun createReceive(): Receive<ApprovalCommand> =
        newReceiveBuilder()
            .onMessage(RequestApproval::class.java, this::onRequestApproval)
            .onMessage(SubmitApproval::class.java, this::onSubmitApproval)
            .onMessage(GetPendingApprovals::class.java, this::onGetPending)
            .build()

    private fun onRequestApproval(cmd: RequestApproval): Behavior<ApprovalCommand> {
        logger.info("Approval requested: ${cmd.approvalId} for run=${cmd.runId}, step=${cmd.stepId}")
        pending[cmd.approvalId] = PendingApproval(
            approvalId = cmd.approvalId,
            runId = cmd.runId,
            stepId = cmd.stepId,
            approvers = cmd.approvers,
            runEntity = cmd.runEntity,
        )
        return this
    }

    private fun onSubmitApproval(cmd: SubmitApproval): Behavior<ApprovalCommand> {
        val approval = pending.remove(cmd.approvalId)
        if (approval == null) {
            cmd.replyTo.tell(ApprovalManagerResponse(false, "Approval '${cmd.approvalId}' not found"))
            return this
        }

        logger.info("Approval ${if (cmd.approved) "granted" else "rejected"}: ${cmd.approvalId}")
        approval.runEntity.tell(
            ApprovalResponse(
                stepId = approval.stepId,
                approvalId = cmd.approvalId,
                approved = cmd.approved,
                approver = cmd.approver,
                reason = cmd.reason,
            )
        )
        cmd.replyTo.tell(ApprovalManagerResponse(true, "Approval processed"))
        return this
    }

    private fun onGetPending(cmd: GetPendingApprovals): Behavior<ApprovalCommand> {
        val infos = pending.values.map {
            PendingApprovalInfo(
                approvalId = it.approvalId,
                runId = it.runId,
                stepId = it.stepId,
                approvers = it.approvers,
                requestedAt = it.requestedAt,
            )
        }
        cmd.replyTo.tell(PendingApprovalsResponse(infos))
        return this
    }
}
