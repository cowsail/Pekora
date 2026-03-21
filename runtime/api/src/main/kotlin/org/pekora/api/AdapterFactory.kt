/**
 * Factory that constructs [AgentRuntimeAdapter] instances from HOCON configuration.
 *
 * Reads the `pekora.adapters.instances` config block and creates one adapter per named entry.
 * The entry key becomes the adapter's [backendId] and the value referenced in workflow YAML
 * `agents[].backend` fields. Multiple instances of the same adapter type (e.g., two LangGraph
 * servers with different endpoints) are declared as separate named entries:
 *
 * ```hocon
 * pekora.adapters.instances {
 *   langgraph-prod    { type = langgraph, enabled = true, service-url = "...", api-key = "..." }
 *   langgraph-staging { type = langgraph, enabled = true, service-url = "...", api-key = "..." }
 *   my-a2a            { type = a2a,       enabled = true, service-url = "..." }
 * }
 * ```
 *
 * Supported types: `langgraph`, `a2a`, `bedrock-agentcore`, `generic`.
 *
 * The native adapter is always included when calling the three-argument overload and
 * `pekora.adapters.native.enabled` is `true` (default).
 *
 * @see AgentRuntimeAdapter
 * @see FrameworkServer
 */
package org.pekora.api

import com.typesafe.config.Config
import org.apache.pekko.actor.typed.ActorSystem
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.a2a.A2AAdapter
import org.pekora.adapters.bedrockagentcore.BedrockAgentCoreAdapter
import org.pekora.adapters.generic.GenericAdapter
import org.pekora.adapters.langgraph.LangGraphAdapter
import org.pekora.adapters.native.NativeAdapter
import org.pekora.adapters.native.NativeAgentRegistry
import org.slf4j.LoggerFactory

object AdapterFactory {

    private val logger = LoggerFactory.getLogger(AdapterFactory::class.java)

    /**
     * Reads `pekora.adapters` from [config] and constructs enabled adapters, including
     * the [NativeAdapter] backed by [nativeAgents].
     *
     * @param config The root Pekko/Pekora config loaded at startup.
     * @param system The actor system — required to spawn native agent actors.
     * @param nativeAgents The registry of native Pekko agent behaviors.
     * @return Map of backend identifier to adapter instance.
     */
    fun createAdapters(
        config: Config,
        system: ActorSystem<*>,
        nativeAgents: NativeAgentRegistry,
    ): Map<String, AgentRuntimeAdapter> {
        val result = createAdapters(config).toMutableMap()
        val adaptersConfig = if (config.hasPath("pekora.adapters")) config.getConfig("pekora.adapters") else null
        val nativeEnabled = adaptersConfig?.let {
            !it.hasPath("native.enabled") || it.getBoolean("native.enabled")
        } ?: true

        if (nativeEnabled) {
            val adapter = NativeAdapter(nativeAgents, system)
            result[adapter.backendId] = adapter
            logger.info("Native adapter enabled (registered agents: ${nativeAgents.registeredNames()})")
        }
        return result
    }

    /**
     * Reads `pekora.adapters.instances` from [config] and constructs enabled adapters.
     *
     * Does not include the native adapter — use the three-argument overload for that.
     *
     * @param config The root Pekko/Pekora config loaded at startup.
     * @return Map of instance name to adapter instance.
     */
    fun createAdapters(config: Config): Map<String, AgentRuntimeAdapter> {
        val adaptersConfig = if (config.hasPath("pekora.adapters")) {
            config.getConfig("pekora.adapters")
        } else {
            logger.warn("No pekora.adapters config found; using default adapter configurations")
            return defaultAdapters()
        }

        if (!adaptersConfig.hasPath("instances")) {
            logger.warn("No pekora.adapters.instances config found; using default adapter configurations")
            return defaultAdapters()
        }

        val instancesConfig = adaptersConfig.getConfig("instances")
        val result = mutableMapOf<String, AgentRuntimeAdapter>()

        for (instanceName in instancesConfig.root().keys) {
            val cfg = instancesConfig.getConfig(instanceName)

            if (cfg.hasPath("enabled") && !cfg.getBoolean("enabled")) {
                logger.debug("Adapter instance '{}' is disabled — skipping", instanceName)
                continue
            }

            val adapterType = if (cfg.hasPath("type")) cfg.getString("type") else {
                logger.warn("Adapter instance '{}' missing 'type' field — skipping", instanceName)
                continue
            }

            val adapter: AgentRuntimeAdapter? = when (adapterType) {
                "langgraph" -> LangGraphAdapter(
                    backendId = instanceName,
                    serviceUrl = cfg.getString("service-url"),
                    apiKey = cfg.getString("api-key"),
                )
                "a2a" -> A2AAdapter(
                    backendId = instanceName,
                    serviceUrl = cfg.getString("service-url"),
                    apiKey = cfg.getString("api-key"),
                )
                "bedrock-agentcore" -> BedrockAgentCoreAdapter(
                    backendId = instanceName,
                    agentRuntimeArn = cfg.getString("agent-runtime-arn"),
                    region = cfg.getString("region"),
                    authMode = cfg.getString("auth-mode"),
                    oauthToken = cfg.getString("oauth-token"),
                )
                "generic" -> GenericAdapter.http(
                    backendId = instanceName,
                    baseUrl = cfg.getString("service-url"),
                    apiKey = cfg.getString("api-key"),
                )
                else -> {
                    logger.warn("Unknown adapter type '{}' for instance '{}' — skipping", adapterType, instanceName)
                    null
                }
            }

            if (adapter != null) {
                result[instanceName] = adapter
                logger.info("Adapter instance '{}' enabled (type={})", instanceName, adapterType)
            }
        }

        return result
    }

    private fun defaultAdapters(): Map<String, AgentRuntimeAdapter> = mapOf(
        "langgraph-default" to LangGraphAdapter(backendId = "langgraph-default"),
        "a2a-default" to A2AAdapter(backendId = "a2a-default"),
        "bedrock-default" to BedrockAgentCoreAdapter(backendId = "bedrock-default"),
        "generic-default" to GenericAdapter.http("generic-default", "http://localhost:8400"),
    )
}
