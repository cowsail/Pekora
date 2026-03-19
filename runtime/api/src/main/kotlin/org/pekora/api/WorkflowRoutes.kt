/**
 * HTTP route definitions for workflow template and version management (Section 15.1).
 *
 * This file defines the Definition API surface of the Pekko Agent Workflow Framework.
 * It exposes RESTful endpoints for registering workflow templates, publishing new
 * versions, listing templates, and retrieving specific version metadata. All routes
 * delegate to the [WorkflowRegistry][org.pekora.registry.WorkflowRegistry] actor
 * via the Pekko ask pattern.
 *
 * **Endpoints provided:**
 *
 * | Method | Path                                       | Description                    |
 * |--------|--------------------------------------------|--------------------------------|
 * | POST   | `/workflow-templates`                      | Register a new template        |
 * | GET    | `/workflow-templates`                      | List all registered templates  |
 * | POST   | `/workflow-templates/{id}/versions`         | Publish a new version          |
 * | GET    | `/workflow-versions/{templateId:version}`   | Retrieve a specific version    |
 *
 * @see WorkflowRoutes
 * @see org.pekora.registry.WorkflowRegistry
 */
package org.pekora.api

import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.javadsl.AskPattern
import org.apache.pekko.http.javadsl.marshallers.jackson.Jackson
import org.apache.pekko.http.javadsl.model.StatusCodes
import org.apache.pekko.http.javadsl.server.AllDirectives
import org.apache.pekko.http.javadsl.server.PathMatchers
import org.apache.pekko.http.javadsl.server.Route
import org.pekora.registry.*
import org.pekora.dsl.WorkflowParser
import java.time.Duration

/**
 * Pekko HTTP route handler for the Definition API (Section 15.1).
 *
 * Provides RESTful endpoints for managing workflow templates and their versions.
 * Communication with the [WorkflowRegistry][org.pekora.registry.WorkflowRegistry]
 * actor is performed asynchronously using the Pekko typed ask pattern with a
 * configurable timeout.
 *
 * @property registry An [ActorRef] to the [WorkflowRegistry][org.pekora.registry.WorkflowRegistry]
 *   actor that stores template and version metadata.
 * @property system The Pekko [ActorSystem] used to obtain the scheduler for ask-pattern calls.
 * @see org.pekora.registry.RegistryCommand
 * @see RunRoutes
 */
class WorkflowRoutes(
    private val registry: ActorRef<RegistryCommand>,
    private val system: ActorSystem<*>,
) : AllDirectives() {

    private val askTimeout = Duration.ofSeconds(5)

    /**
     * Builds and returns the composite [Route] for all workflow definition endpoints.
     *
     * The returned route tree handles:
     * - **POST /workflow-templates** -- registers a new workflow template.
     * - **GET  /workflow-templates** -- lists all registered templates.
     * - **POST /workflow-templates/{id}/versions** -- publishes a new version for an
     *   existing template. The request body contains the raw YAML workflow definition
     *   which is parsed via [WorkflowParser].
     * - **GET  /workflow-versions/{templateId:version}** -- retrieves a specific
     *   published version. The `versionId` path segment must use the format
     *   `templateId:version` (e.g., `"my-workflow:3"`).
     *
     * @return A Pekko HTTP [Route] that can be concatenated with other route trees.
     * @see WorkflowParser
     * @see org.pekora.registry.RegisterTemplate
     * @see org.pekora.registry.PublishVersion
     * @see org.pekora.registry.ListTemplates
     * @see org.pekora.registry.GetVersion
     */
    fun routes(): Route = concat(
        // POST /workflow-templates
        pathPrefix("workflow-templates") {
            concat(
                pathEnd {
                    concat(
                        post {
                            entity(Jackson.unmarshaller(CreateTemplateRequest::class.java)) { req ->
                                val future = AskPattern.ask(
                                    registry,
                                    { replyTo: ActorRef<RegistryResponse> ->
                                        RegisterTemplate(
                                            id = req.id,
                                            name = req.name,
                                            description = req.description,
                                            owner = req.owner,
                                            tenantId = req.tenantId,
                                            replyTo = replyTo,
                                        )
                                    },
                                    askTimeout,
                                    system.scheduler(),
                                )
                                onSuccess(future) { response ->
                                    if (response.success) {
                                        complete(StatusCodes.CREATED, response, Jackson.marshaller())
                                    } else {
                                        complete(StatusCodes.CONFLICT, response, Jackson.marshaller())
                                    }
                                }
                            }
                        },
                        get {
                            val future = AskPattern.ask(
                                registry,
                                { replyTo: ActorRef<TemplateListResponse> ->
                                    ListTemplates(replyTo = replyTo)
                                },
                                askTimeout,
                                system.scheduler(),
                            )
                            onSuccess(future) { response ->
                                complete(StatusCodes.OK, response, Jackson.marshaller())
                            }
                        },
                    )
                },
                // POST /workflow-templates/{id}/versions
                path(PathMatchers.segment().slash("versions")) { templateId ->
                    post {
                        entity(Jackson.unmarshaller(PublishVersionRequest::class.java)) { req ->
                            val definition = WorkflowParser.parse(req.workflowYaml)
                            val future = AskPattern.ask(
                                registry,
                                { replyTo: ActorRef<RegistryResponse> ->
                                    PublishVersion(
                                        templateId = templateId,
                                        version = req.version,
                                        definition = definition,
                                        replyTo = replyTo,
                                    )
                                },
                                askTimeout,
                                system.scheduler(),
                            )
                            onSuccess(future) { response ->
                                if (response.success) {
                                    complete(StatusCodes.CREATED, response, Jackson.marshaller())
                                } else {
                                    complete(StatusCodes.BAD_REQUEST, response, Jackson.marshaller())
                                }
                            }
                        }
                    }
                },
            )
        },
        // GET /workflow-versions/{id}
        path(PathMatchers.segment("workflow-versions").slash(PathMatchers.segment())) { versionId ->
            get {
                // versionId format: templateId:version
                val parts = versionId.split(":")
                if (parts.size != 2) {
                    complete(StatusCodes.BAD_REQUEST, "Invalid version ID format. Use templateId:version")
                } else {
                    val future = AskPattern.ask(
                        registry,
                        { replyTo: ActorRef<VersionResponse> ->
                            GetVersion(
                                templateId = parts[0],
                                version = parts[1].toIntOrNull() ?: 0,
                                replyTo = replyTo,
                            )
                        },
                        askTimeout,
                        system.scheduler(),
                    )
                    onSuccess(future) { response ->
                        if (response.found) {
                            complete(StatusCodes.OK, response.version, Jackson.marshaller())
                        } else {
                            complete(StatusCodes.NOT_FOUND, "Version not found")
                        }
                    }
                }
            }
        },
    )
}

// --- Request DTOs ---

/**
 * Request body for `POST /workflow-templates` to register a new workflow template.
 *
 * @property id Unique identifier for the template (e.g., `"customer-onboarding"`).
 * @property name Human-readable display name.
 * @property description Optional description of what the workflow does.
 * @property owner Optional owner or team responsible for this template.
 * @property tenantId Optional tenant identifier for multi-tenant deployments.
 * @see WorkflowRoutes.routes
 */
data class CreateTemplateRequest(
    val id: String = "",
    val name: String = "",
    val description: String = "",
    val owner: String = "",
    val tenantId: String = "",
)

/**
 * Request body for `POST /workflow-templates/{id}/versions` to publish a new
 * workflow version.
 *
 * @property version The integer version number to assign (e.g., `1`, `2`, `3`).
 * @property workflowYaml The raw YAML string containing the workflow definition.
 *   This is parsed by [WorkflowParser] before being stored in the registry.
 * @see WorkflowRoutes.routes
 * @see WorkflowParser
 */
data class PublishVersionRequest(
    val version: Int = 1,
    val workflowYaml: String = "",
)
