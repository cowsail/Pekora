/**
 * Amazon Bedrock AgentCore adapter for the Pekora framework.
 *
 * Invokes agents deployed on Amazon Bedrock AgentCore via its REST API:
 * `POST /runtimes/{agentRuntimeArn}/invocations`
 *
 * Supports two authentication modes:
 * - `"oauth"`: `Authorization: Bearer <oauthToken>` header.
 * - `"sigv4"`: AWS Signature Version 4 signing via the AWS SDK v2 auth module,
 *   using credentials from the [DefaultCredentialsProvider] chain.
 *
 * Phase 2 scope: non-streaming (`Accept: application/json`). Single invocation per step.
 *
 * @see org.pekora.adapters.AgentRuntimeAdapter
 */
package org.pekora.adapters.bedrockagentcore

import kotlinx.serialization.json.*
import org.pekora.adapters.AdapterHealth
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.HealthStatus
import org.pekora.dsl.StepExecutionRequest
import org.pekora.dsl.StepExecutionResult
import org.pekora.dsl.StepResultStatus
import org.pekora.dsl.ToolCallRecord
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.auth.signer.Aws4Signer
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams
import software.amazon.awssdk.http.SdkHttpFullRequest
import software.amazon.awssdk.http.SdkHttpMethod
import software.amazon.awssdk.regions.Region
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Adapter that dispatches workflow steps to Amazon Bedrock AgentCore.
 *
 * @property agentRuntimeArn ARN of the AgentCore runtime (e.g.,
 *   `"arn:aws:bedrock-agentcore:us-east-1:123456789012:agent-runtime/my-runtime"`).
 * @property region AWS region (e.g., `"us-east-1"`).
 * @property authMode Authentication mode: `"sigv4"` or `"oauth"`.
 * @property oauthToken Bearer token used when [authMode] is `"oauth"`.
 * @property httpClient JDK HTTP client used for all outbound requests.
 */
class BedrockAgentCoreAdapter(
    override val backendId: String = "bedrock-agentcore",
    private val agentRuntimeArn: String = "",
    private val region: String = "us-east-1",
    private val authMode: String = "oauth",
    private val oauthToken: String = "",
    private val httpClient: HttpClient = HttpClient.newHttpClient(),
    internal val endpointOverride: String? = null,
) : AgentRuntimeAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(BedrockAgentCoreAdapter::class.java)
        private val json = Json { ignoreUnknownKeys = true }
        private const val SERVICE_NAME = "bedrock-agentcore"
    }

    private val endpointBase: String
        get() = endpointOverride ?: "https://bedrock-agentcore.$region.amazonaws.com"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        if (agentRuntimeArn.isEmpty()) {
            return CompletableFuture.completedFuture(
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "BedrockAgentCore: agent-runtime-arn is not configured",
                )
            )
        }

        val promptText = request.input.entries.joinToString("\n") { (k, v) -> "$k: $v" }
        val body = buildJsonObject {
            put("prompt", promptText)
            put("runtimeSessionId", "${request.runId}_${request.stepId}")
            put("context", buildJsonObject {
                put("runId", request.runId)
                put("stepId", request.stepId)
            })
        }.toString()

        val encodedArn = URLEncoder.encode(agentRuntimeArn, "UTF-8")
        val url = "$endpointBase/runtimes/$encodedArn/invocations"

        return try {
            val httpRequest = buildSignedRequest(url, body)
            httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString())
                .thenApply { response ->
                    if (response.statusCode() !in 200..299) {
                        logger.error("BedrockAgentCore returned HTTP ${response.statusCode()}: ${response.body()}")
                        StepExecutionResult(
                            status = StepResultStatus.FAILED,
                            error = "BedrockAgentCore error: HTTP ${response.statusCode()}",
                        )
                    } else {
                        parseResponse(response.body())
                    }
                }
                .exceptionally { ex ->
                    logger.error("BedrockAgentCore step ${request.stepId} failed", ex)
                    StepExecutionResult(
                        status = StepResultStatus.FAILED,
                        error = "BedrockAgentCore execution failed: ${ex.message}",
                    )
                }
        } catch (ex: Exception) {
            logger.error("Failed to build BedrockAgentCore request", ex)
            CompletableFuture.completedFuture(
                StepExecutionResult(
                    status = StepResultStatus.FAILED,
                    error = "Failed to build request: ${ex.message}",
                )
            )
        }
    }

    override fun healthCheck(): CompletionStage<AdapterHealth> {
        if (agentRuntimeArn.isEmpty()) {
            return CompletableFuture.completedFuture(
                AdapterHealth(backendId, HealthStatus.UNKNOWN, "agent-runtime-arn not configured")
            )
        }
        val start = System.currentTimeMillis()
        val req = HttpRequest.newBuilder()
            .uri(URI.create("$endpointBase/ping"))
            .timeout(Duration.ofSeconds(3))
            .GET()
            .apply { if (authMode == "oauth" && oauthToken.isNotEmpty()) header("Authorization", "Bearer $oauthToken") }
            .build()
        return httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                val latency = System.currentTimeMillis() - start
                if (response.statusCode() in 200..299) {
                    AdapterHealth(backendId, HealthStatus.HEALTHY, latencyMs = latency)
                } else {
                    AdapterHealth(backendId, HealthStatus.UNHEALTHY, "HTTP ${response.statusCode()}", latency)
                }
            }
            .exceptionally { ex ->
                AdapterHealth(backendId, HealthStatus.UNHEALTHY, ex.message ?: "connection failed")
            }
    }

    private fun buildSignedRequest(url: String, body: String): HttpRequest {
        return if (authMode == "sigv4") {
            buildSigV4Request(url, body)
        } else {
            buildOAuthRequest(url, body)
        }
    }

    private fun buildOAuthRequest(url: String, body: String): HttpRequest =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
            .apply { if (oauthToken.isNotEmpty()) header("Authorization", "Bearer $oauthToken") }
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()

    private fun buildSigV4Request(url: String, body: String): HttpRequest {
        val bodyBytes = body.toByteArray(Charsets.UTF_8)
        val uri = URI.create(url)

        val sdkRequest = SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.POST)
            .uri(uri)
            .putHeader("Content-Type", "application/json")
            .putHeader("Accept", "application/json")
            .contentStreamProvider { ByteArrayInputStream(bodyBytes) }
            .build()

        val credentials = DefaultCredentialsProvider.create().resolveCredentials()
        val signerParams = Aws4SignerParams.builder()
            .awsCredentials(credentials)
            .signingName(SERVICE_NAME)
            .signingRegion(Region.of(region))
            .build()

        val signedSdkRequest = Aws4Signer.create().sign(sdkRequest, signerParams)

        val builder = HttpRequest.newBuilder()
            .uri(uri)
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
        signedSdkRequest.headers().forEach { (name, values) ->
            values.forEach { value -> builder.header(name, value) }
        }
        return builder.build()
    }

    private fun parseResponse(body: String): StepExecutionResult {
        return try {
            val root = json.parseToJsonElement(body).jsonObject
            val message = root["response"]?.jsonObject?.get("message")?.jsonPrimitive?.content
                ?: root["message"]?.jsonPrimitive?.content
                ?: ""

            val toolCalls = root["response"]?.jsonObject?.get("tool_calls")?.jsonArray?.mapNotNull { tc ->
                try {
                    val obj = tc.jsonObject
                    val inputMap = obj["input"]?.jsonObject?.entries
                        ?.mapNotNull { (k, v) -> try { k to v.jsonPrimitive.content } catch (_: Exception) { null } }
                        ?.toMap() ?: emptyMap()
                    val outputMap = obj["output"]?.jsonObject?.entries
                        ?.mapNotNull { (k, v) -> try { k to v.jsonPrimitive.content } catch (_: Exception) { null } }
                        ?.toMap() ?: obj["output"]?.let { mapOf("result" to it.toString()) } ?: emptyMap()
                    ToolCallRecord(
                        tool = obj["name"]?.jsonPrimitive?.content ?: "",
                        input = inputMap,
                        output = outputMap,
                    )
                } catch (_: Exception) { null }
            } ?: emptyList()

            StepExecutionResult(
                status = StepResultStatus.SUCCEEDED,
                output = if (message.isNotEmpty()) mapOf("result" to message) else emptyMap(),
                toolCalls = toolCalls,
            )
        } catch (e: Exception) {
            logger.error("Failed to parse BedrockAgentCore response", e)
            StepExecutionResult(
                status = StepResultStatus.FAILED,
                error = "Failed to parse response: ${e.message}",
            )
        }
    }
}
