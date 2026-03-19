/**
 * Workflow template and version registry for the Pekko Agent Workflow Framework.
 *
 * This file implements the workflow registry described in **Section 6.2** of the
 * framework specification. The registry is responsible for storing workflow templates
 * and their published versions, and for resolving version metadata at run-creation time.
 *
 * **v1 design**: The current implementation is a single Pekko Typed actor that holds all
 * template and version data in memory. This is sufficient for development and
 * single-node deployments. In future versions the backing store can be replaced with a
 * persistent database without changing the command/response protocol.
 *
 * The registry communicates via a sealed [RegistryCommand] protocol and replies with
 * [RegistryResponse], [TemplateResponse], [VersionResponse], or [TemplateListResponse]
 * depending on the command.
 *
 * @see RegistryCommand
 * @see WorkflowRegistry
 */
package org.pekora.registry

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior
import org.apache.pekko.actor.typed.javadsl.ActorContext
import org.apache.pekko.actor.typed.javadsl.Behaviors
import org.apache.pekko.actor.typed.javadsl.Receive
import org.pekora.dsl.WorkflowDefinition
import org.pekora.dsl.WorkflowTemplate
import org.pekora.dsl.WorkflowVersion
import org.slf4j.LoggerFactory
import java.time.Instant

// --- Commands ---

/**
 * Sealed command protocol for the [WorkflowRegistry] actor (Section 6.2).
 *
 * All interactions with the registry are modeled as typed messages that implement
 * this interface. Each command carries a `replyTo` [ActorRef] so the registry can
 * send a response back to the caller.
 *
 * @see RegisterTemplate
 * @see PublishVersion
 * @see GetTemplate
 * @see GetVersion
 * @see GetLatestVersion
 * @see ListTemplates
 */
sealed interface RegistryCommand

/**
 * Command to register a new workflow template in the registry.
 *
 * Registration is idempotent-safe: if a template with the same [id] already exists
 * the registry replies with a [RegistryError].
 *
 * @property id Unique identifier for the template (e.g., `"customer-onboarding"`).
 * @property name Human-readable display name for the template.
 * @property description Optional longer description of the template's purpose.
 * @property owner Optional owner or team responsible for this template.
 * @property tenantId Optional tenant identifier for multi-tenant deployments.
 * @property replyTo The actor to receive the [RegistryResponse] (success or error).
 * @see RegistryResponse
 */
data class RegisterTemplate(
    val id: String,
    val name: String,
    val description: String = "",
    val owner: String = "",
    val tenantId: String = "",
    val replyTo: ActorRef<RegistryResponse>,
) : RegistryCommand

/**
 * Command to publish a new version of an existing workflow template.
 *
 * The template identified by [templateId] must already be registered. Version numbers
 * must be unique within a template; publishing a duplicate version number results in
 * a [RegistryError].
 *
 * @property templateId The identifier of the parent template.
 * @property version The integer version number to publish (e.g., `1`, `2`, `3`).
 * @property definition The fully parsed [WorkflowDefinition] for this version.
 * @property replyTo The actor to receive the [RegistryResponse] (success or error).
 * @see WorkflowDefinition
 * @see RegistryResponse
 */
data class PublishVersion(
    val templateId: String,
    val version: Int,
    val definition: WorkflowDefinition,
    val replyTo: ActorRef<RegistryResponse>,
) : RegistryCommand

/**
 * Command to retrieve a workflow template by its identifier.
 *
 * @property templateId The unique identifier of the template to look up.
 * @property replyTo The actor to receive the [TemplateResponse].
 * @see TemplateResponse
 */
data class GetTemplate(
    val templateId: String,
    val replyTo: ActorRef<TemplateResponse>,
) : RegistryCommand

/**
 * Command to retrieve a specific version of a workflow template.
 *
 * @property templateId The identifier of the parent template.
 * @property version The exact version number to retrieve.
 * @property replyTo The actor to receive the [VersionResponse].
 * @see VersionResponse
 */
data class GetVersion(
    val templateId: String,
    val version: Int,
    val replyTo: ActorRef<VersionResponse>,
) : RegistryCommand

/**
 * Command to retrieve the latest (highest-numbered) published version of a template.
 *
 * @property templateId The identifier of the parent template.
 * @property replyTo The actor to receive the [VersionResponse].
 * @see VersionResponse
 */
data class GetLatestVersion(
    val templateId: String,
    val replyTo: ActorRef<VersionResponse>,
) : RegistryCommand

/**
 * Command to list all registered templates, optionally filtered by tenant.
 *
 * @property tenantId When non-empty, only templates belonging to this tenant are returned.
 *   When empty, all templates are returned regardless of tenant.
 * @property replyTo The actor to receive the [TemplateListResponse].
 * @see TemplateListResponse
 */
data class ListTemplates(
    val tenantId: String = "",
    val replyTo: ActorRef<TemplateListResponse>,
) : RegistryCommand

// --- Responses ---

/**
 * Common response interface for registry mutation commands ([RegisterTemplate],
 * [PublishVersion]).
 *
 * @property success `true` if the command was processed successfully.
 * @property message A human-readable status or error message.
 * @see RegistryOk
 * @see RegistryError
 */
sealed interface RegistryResponse {
    val success: Boolean
    val message: String
}

/**
 * Successful registry response.
 *
 * @property success Always `true`.
 * @property message A confirmation message describing the completed operation.
 */
data class RegistryOk(
    override val success: Boolean = true,
    override val message: String = "OK",
) : RegistryResponse

/**
 * Error registry response, returned when a command cannot be fulfilled.
 *
 * @property success Always `false`.
 * @property message A human-readable description of the error (e.g., duplicate template).
 */
data class RegistryError(
    override val success: Boolean = false,
    override val message: String,
) : RegistryResponse

/**
 * Response to a [GetTemplate] command.
 *
 * @property template The resolved [WorkflowTemplate], or `null` if not found.
 * @property found `true` if a template with the requested identifier exists.
 * @see GetTemplate
 * @see WorkflowTemplate
 */
data class TemplateResponse(
    val template: WorkflowTemplate?,
    val found: Boolean,
)

/**
 * Response to a [GetVersion] or [GetLatestVersion] command.
 *
 * @property version The resolved [WorkflowVersion], or `null` if not found.
 * @property found `true` if the requested version exists.
 * @see GetVersion
 * @see GetLatestVersion
 * @see WorkflowVersion
 */
data class VersionResponse(
    val version: WorkflowVersion?,
    val found: Boolean,
)

/**
 * Response to a [ListTemplates] command containing all matching templates.
 *
 * @property templates The list of [WorkflowTemplate] objects matching the query.
 * @see ListTemplates
 * @see WorkflowTemplate
 */
data class TemplateListResponse(
    val templates: List<WorkflowTemplate>,
)

/**
 * In-memory workflow registry actor (Section 6.2).
 *
 * This Pekko Typed [AbstractBehavior] manages the lifecycle of workflow templates and
 * their published versions. It stores all data in two in-memory maps:
 *
 * - **templates**: maps template identifiers to [WorkflowTemplate] metadata.
 * - **versions**: maps template identifiers to an ordered list of [WorkflowVersion] entries.
 *
 * The actor processes [RegistryCommand] messages and replies via the `replyTo` actor
 * reference embedded in each command. All operations are synchronous within the actor
 * and therefore inherently thread-safe.
 *
 * In a future version (v2+), the in-memory maps can be replaced with a persistent store
 * (e.g., a relational database or Pekko Persistence) without modifying the external
 * command/response protocol.
 *
 * @param context The Pekko actor context provided during actor creation.
 * @see RegistryCommand
 * @see RegistryResponse
 */
class WorkflowRegistry(
    context: ActorContext<RegistryCommand>,
) : AbstractBehavior<RegistryCommand>(context) {

    companion object {
        private val logger = LoggerFactory.getLogger(WorkflowRegistry::class.java)

        /**
         * Factory method that creates a new [WorkflowRegistry] behavior.
         *
         * @return A [Behavior] suitable for spawning via `context.spawn(...)`.
         * @see WorkflowRegistry
         */
        fun create(): Behavior<RegistryCommand> = Behaviors.setup { ctx ->
            WorkflowRegistry(ctx)
        }
    }

    private val templates = mutableMapOf<String, WorkflowTemplate>()
    private val versions = mutableMapOf<String, MutableList<WorkflowVersion>>()

    override fun createReceive(): Receive<RegistryCommand> =
        newReceiveBuilder()
            .onMessage(RegisterTemplate::class.java, this::onRegisterTemplate)
            .onMessage(PublishVersion::class.java, this::onPublishVersion)
            .onMessage(GetTemplate::class.java, this::onGetTemplate)
            .onMessage(GetVersion::class.java, this::onGetVersion)
            .onMessage(GetLatestVersion::class.java, this::onGetLatestVersion)
            .onMessage(ListTemplates::class.java, this::onListTemplates)
            .build()

    private fun onRegisterTemplate(cmd: RegisterTemplate): Behavior<RegistryCommand> {
        if (templates.containsKey(cmd.id)) {
            cmd.replyTo.tell(RegistryError(message = "Template '${cmd.id}' already exists"))
            return this
        }

        val template = WorkflowTemplate(
            id = cmd.id,
            name = cmd.name,
            description = cmd.description,
            createdAt = Instant.now().toString(),
            owner = cmd.owner,
            tenantId = cmd.tenantId,
        )
        templates[cmd.id] = template
        versions[cmd.id] = mutableListOf()
        logger.info("Registered template: ${cmd.id}")
        cmd.replyTo.tell(RegistryOk(message = "Template registered: ${cmd.id}"))
        return this
    }

    private fun onPublishVersion(cmd: PublishVersion): Behavior<RegistryCommand> {
        if (!templates.containsKey(cmd.templateId)) {
            cmd.replyTo.tell(RegistryError(message = "Template '${cmd.templateId}' not found"))
            return this
        }

        val existing = versions[cmd.templateId]!!
        if (existing.any { it.version == cmd.version }) {
            cmd.replyTo.tell(RegistryError(message = "Version ${cmd.version} already published for ${cmd.templateId}"))
            return this
        }

        val wv = WorkflowVersion(
            templateId = cmd.templateId,
            version = cmd.version,
            definition = cmd.definition,
            publishedAt = Instant.now().toString(),
        )
        existing.add(wv)
        logger.info("Published version ${cmd.version} for template ${cmd.templateId}")
        cmd.replyTo.tell(RegistryOk(message = "Version ${cmd.version} published"))
        return this
    }

    private fun onGetTemplate(cmd: GetTemplate): Behavior<RegistryCommand> {
        val template = templates[cmd.templateId]
        cmd.replyTo.tell(TemplateResponse(template, template != null))
        return this
    }

    private fun onGetVersion(cmd: GetVersion): Behavior<RegistryCommand> {
        val versionList = versions[cmd.templateId]
        val version = versionList?.find { it.version == cmd.version }
        cmd.replyTo.tell(VersionResponse(version, version != null))
        return this
    }

    private fun onGetLatestVersion(cmd: GetLatestVersion): Behavior<RegistryCommand> {
        val versionList = versions[cmd.templateId]
        val latest = versionList?.maxByOrNull { it.version }
        cmd.replyTo.tell(VersionResponse(latest, latest != null))
        return this
    }

    private fun onListTemplates(cmd: ListTemplates): Behavior<RegistryCommand> {
        val filtered = if (cmd.tenantId.isNotEmpty()) {
            templates.values.filter { it.tenantId == cmd.tenantId }
        } else {
            templates.values.toList()
        }
        cmd.replyTo.tell(TemplateListResponse(filtered))
        return this
    }
}
