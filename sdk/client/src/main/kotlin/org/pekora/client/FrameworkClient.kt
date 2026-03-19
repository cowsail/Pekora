/**
 * HTTP client SDK for the Pekko Agent Workflow Framework.
 *
 * This file provides the [FrameworkClient] class, a lightweight, non-blocking HTTP
 * client that wraps the framework's REST API. It is intended for use by external
 * applications, CLI tools, and integration tests that need to interact with the
 * framework programmatically.
 *
 * All methods return [CompletionStage]<[String]> containing the raw JSON response body,
 * making the client easy to compose with other asynchronous APIs.
 *
 * @see FrameworkClient
 */
package org.pekora.client

import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.concurrent.CompletionStage

/**
 * HTTP client SDK for the Pekko Agent Workflow Framework.
 *
 * Provides programmatic, non-blocking access to all framework REST APIs including
 * workflow template management, run lifecycle operations, and approval workflows.
 *
 * **Usage example:**
 * ```kotlin
 * val client = FrameworkClient("http://localhost:8080")
 *
 * // Register a template and publish a version
 * client.createTemplate(id = "my-workflow", name = "My Workflow")
 *     .thenCompose { client.publishVersion("my-workflow", version = 1, workflowYaml = yamlString) }
 *     .thenAccept { println("Version published: $it") }
 *     .join()
 *
 * // Create and start a run
 * client.createRun(templateId = "my-workflow", inputs = mapOf("key" to "value"))
 *     .thenAccept { println("Run created: $it") }
 *     .join()
 *
 * // Check run status
 * client.getRunStatus("run_abc123")
 *     .thenAccept { println("Status: $it") }
 *     .join()
 *
 * // Handle approvals
 * client.getPendingApprovals()
 *     .thenAccept { println("Pending: $it") }
 *     .join()
 *
 * client.approve(approvalId = "approval_xyz", approver = "admin@example.com")
 *     .thenAccept { println("Approved: $it") }
 *     .join()
 * ```
 *
 * @property baseUrl The root URL of the framework server (e.g., `"http://localhost:8080"`).
 *   Defaults to `"http://localhost:8080"`.
 * @property httpClient The [HttpClient] instance used for all HTTP requests. Defaults to
 *   a new client created via [HttpClient.newHttpClient].
 * @see org.pekora.api.WorkflowRoutes
 * @see org.pekora.api.RunRoutes
 */
class FrameworkClient(
    private val baseUrl: String = "http://localhost:8080",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
) {
    private val json = Json { ignoreUnknownKeys = true; prettyPrint = false }

    // --- Workflow Template APIs ---

    /**
     * Registers a new workflow template in the framework.
     *
     * Sends a `POST /workflow-templates` request.
     *
     * @param id Unique identifier for the template (e.g., `"customer-onboarding"`).
     * @param name Human-readable display name for the template.
     * @param description Optional description of the template's purpose.
     * @param owner Optional owner or team responsible for this template.
     * @param tenantId Optional tenant identifier for multi-tenant deployments.
     * @return A [CompletionStage] that completes with the raw JSON response body.
     * @see publishVersion
     * @see listTemplates
     */
    fun createTemplate(
        id: String,
        name: String,
        description: String = "",
        owner: String = "",
        tenantId: String = "",
    ): CompletionStage<String> {
        val body = json.encodeToString(
            mapOf(
                "id" to id,
                "name" to name,
                "description" to description,
                "owner" to owner,
                "tenantId" to tenantId,
            )
        )
        return post("/workflow-templates", body)
    }

    /**
     * Publishes a new version of an existing workflow template.
     *
     * Sends a `POST /workflow-templates/{templateId}/versions` request. The
     * [workflowYaml] is the raw YAML string that will be parsed server-side.
     *
     * @param templateId The identifier of the parent template.
     * @param version The integer version number to publish.
     * @param workflowYaml The raw YAML workflow definition string.
     * @return A [CompletionStage] that completes with the raw JSON response body.
     * @see createTemplate
     */
    fun publishVersion(templateId: String, version: Int, workflowYaml: String): CompletionStage<String> {
        val body = json.encodeToString(
            mapOf(
                "version" to version.toString(),
                "workflowYaml" to workflowYaml,
            )
        )
        return post("/workflow-templates/$templateId/versions", body)
    }

    /**
     * Lists all registered workflow templates.
     *
     * Sends a `GET /workflow-templates` request.
     *
     * @return A [CompletionStage] that completes with the raw JSON response body
     *   containing the list of templates.
     * @see createTemplate
     */
    fun listTemplates(): CompletionStage<String> {
        return get("/workflow-templates")
    }

    // --- Run APIs ---

    /**
     * Creates and starts a new workflow run.
     *
     * Sends a `POST /runs` request. The server orchestrates the full create-load-start
     * lifecycle within a single request. If [version] is `0`, the latest published
     * version of the template is used.
     *
     * @param templateId The workflow template to instantiate.
     * @param version The specific version number to use, or `0` for the latest.
     * @param inputs A map of input key-value pairs to pass to the workflow.
     * @param tenantId Optional tenant identifier for multi-tenant deployments.
     * @return A [CompletionStage] that completes with the raw JSON response body
     *   containing the assigned run ID and status message.
     * @see getRunStatus
     * @see cancelRun
     */
    fun createRun(
        templateId: String,
        version: Int = 0,
        inputs: Map<String, String> = emptyMap(),
        tenantId: String = "",
    ): CompletionStage<String> {
        val body = json.encodeToString(
            mapOf(
                "templateId" to templateId,
                "version" to version.toString(),
                "inputs" to inputs.toString(),
                "tenantId" to tenantId,
            )
        )
        return post("/runs", body)
    }

    /**
     * Retrieves the current status of a workflow run.
     *
     * Sends a `GET /runs/{runId}` request.
     *
     * @param runId The unique identifier of the run to query.
     * @return A [CompletionStage] that completes with the raw JSON response body
     *   containing the run's current state, step states, and metadata.
     * @see createRun
     */
    fun getRunStatus(runId: String): CompletionStage<String> {
        return get("/runs/$runId")
    }

    /**
     * Cancels a running workflow.
     *
     * Sends a `POST /runs/{runId}/cancel` request.
     *
     * @param runId The unique identifier of the run to cancel.
     * @return A [CompletionStage] that completes with the raw JSON response body.
     * @see createRun
     * @see resumeRun
     */
    fun cancelRun(runId: String): CompletionStage<String> {
        return post("/runs/$runId/cancel", "")
    }

    /**
     * Resumes a paused workflow run.
     *
     * Sends a `POST /runs/{runId}/resume` request. This is typically used after
     * a run has been paused due to an error or external intervention.
     *
     * @param runId The unique identifier of the run to resume.
     * @return A [CompletionStage] that completes with the raw JSON response body.
     * @see cancelRun
     * @see createRun
     */
    fun resumeRun(runId: String): CompletionStage<String> {
        return post("/runs/$runId/resume", "")
    }

    // --- Approval APIs ---

    /**
     * Lists all pending approval gates across all runs.
     *
     * Sends a `GET /approvals` request.
     *
     * @return A [CompletionStage] that completes with the raw JSON response body
     *   containing the list of pending approval requests.
     * @see approve
     * @see reject
     */
    fun getPendingApprovals(): CompletionStage<String> {
        return get("/approvals")
    }

    /**
     * Approves a pending approval gate.
     *
     * Sends a `POST /approvals/{approvalId}/approve` request. The run that owns
     * the approval gate will resume execution after approval.
     *
     * @param approvalId The unique identifier of the approval gate.
     * @param approver The identity of the person or system approving.
     * @param reason An optional reason or justification for the approval.
     * @return A [CompletionStage] that completes with the raw JSON response body.
     * @see reject
     * @see getPendingApprovals
     */
    fun approve(approvalId: String, approver: String, reason: String = ""): CompletionStage<String> {
        val body = json.encodeToString(mapOf("approver" to approver, "reason" to reason))
        return post("/approvals/$approvalId/approve", body)
    }

    /**
     * Rejects a pending approval gate.
     *
     * Sends a `POST /approvals/{approvalId}/reject` request. The run that owns
     * the approval gate will handle the rejection according to its workflow definition
     * (e.g., marking the step as cancelled or failed).
     *
     * @param approvalId The unique identifier of the approval gate.
     * @param approver The identity of the person or system rejecting.
     * @param reason An optional reason or justification for the rejection.
     * @return A [CompletionStage] that completes with the raw JSON response body.
     * @see approve
     * @see getPendingApprovals
     */
    fun reject(approvalId: String, approver: String, reason: String = ""): CompletionStage<String> {
        val body = json.encodeToString(mapOf("approver" to approver, "reason" to reason))
        return post("/approvals/$approvalId/reject", body)
    }

    // --- HTTP helpers ---

    private fun get(path: String): CompletionStage<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl$path"))
            .GET()
            .build()
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { it.body() }
    }

    private fun post(path: String, body: String): CompletionStage<String> {
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$baseUrl$path"))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        return httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { it.body() }
    }
}
