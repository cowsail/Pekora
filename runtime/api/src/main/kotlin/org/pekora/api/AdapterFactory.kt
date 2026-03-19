/**
 * Factory that constructs [AgentRuntimeAdapter] instances from HOCON configuration.
 *
 * Reads the `pekora.adapters` config block, creates only enabled adapters, and returns
 * a map keyed by backend identifier for use in [FrameworkServer].
 *
 * @see AgentRuntimeAdapter
 * @see FrameworkServer
 */
package org.pekora.api

import com.typesafe.config.Config
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.a2a.A2AAdapter
import org.pekora.adapters.bedrockagentcore.BedrockAgentCoreAdapter
import org.pekora.adapters.generic.GenericAdapter
import org.pekora.adapters.langgraph.LangGraphAdapter
import org.slf4j.LoggerFactory

object AdapterFactory {

    private val logger = LoggerFactory.getLogger(AdapterFactory::class.java)

    /**
     * Reads `pekora.adapters` from [config] and constructs enabled adapters.
     *
     * @param config The root Pekko/Pekora config loaded at startup.
     * @return Map of backend identifier to adapter instance.
     */
    fun createAdapters(config: Config): Map<String, AgentRuntimeAdapter> {
        val adaptersConfig = if (config.hasPath("pekora.adapters")) {
            config.getConfig("pekora.adapters")
        } else {
            logger.warn("No pekora.adapters config found; using default adapter configurations")
            return defaultAdapters()
        }

        val result = mutableMapOf<String, AgentRuntimeAdapter>()

        if (adaptersConfig.hasPath("langgraph") && adaptersConfig.getBoolean("langgraph.enabled")) {
            val cfg = adaptersConfig.getConfig("langgraph")
            val adapter = LangGraphAdapter(
                serviceUrl = cfg.getString("service-url"),
                apiKey = cfg.getString("api-key"),
            )
            result[adapter.backendId] = adapter
            logger.info("LangGraph adapter enabled (url=${cfg.getString("service-url")})")
        }

        if (adaptersConfig.hasPath("a2a") && adaptersConfig.getBoolean("a2a.enabled")) {
            val cfg = adaptersConfig.getConfig("a2a")
            val adapter = A2AAdapter(
                serviceUrl = cfg.getString("service-url"),
                apiKey = cfg.getString("api-key"),
            )
            result[adapter.backendId] = adapter
            logger.info("A2A adapter enabled (url=${cfg.getString("service-url")})")
        }

        if (adaptersConfig.hasPath("bedrock-agentcore") && adaptersConfig.getBoolean("bedrock-agentcore.enabled")) {
            val cfg = adaptersConfig.getConfig("bedrock-agentcore")
            val adapter = BedrockAgentCoreAdapter(
                agentRuntimeArn = cfg.getString("agent-runtime-arn"),
                region = cfg.getString("region"),
                authMode = cfg.getString("auth-mode"),
                oauthToken = cfg.getString("oauth-token"),
            )
            result[adapter.backendId] = adapter
            logger.info("BedrockAgentCore adapter enabled (region=${cfg.getString("region")}, auth=${cfg.getString("auth-mode")})")
        }

        if (adaptersConfig.hasPath("generic") && adaptersConfig.getBoolean("generic.enabled")) {
            val cfg = adaptersConfig.getConfig("generic")
            val adapter = GenericAdapter.http(
                backendId = "generic",
                baseUrl = cfg.getString("service-url"),
                apiKey = cfg.getString("api-key"),
            )
            result[adapter.backendId] = adapter
            logger.info("Generic adapter enabled (url=${cfg.getString("service-url")})")
        }

        return result
    }

    private fun defaultAdapters(): Map<String, AgentRuntimeAdapter> = mapOf(
        "langgraph" to LangGraphAdapter(),
        "a2a" to A2AAdapter(),
        "bedrock-agentcore" to BedrockAgentCoreAdapter(),
        "generic" to GenericAdapter.http("generic", "http://localhost:8400"),
    )
}
