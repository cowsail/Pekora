package org.pekora.api

import org.pekora.adapters.native.NativeAgentRegistry
import org.pekora.framework.PekoraFramework
import org.pekora.framework.PekoraFrameworkOptions

object FrameworkServer {
    val nativeAgents: NativeAgentRegistry = NativeAgentRegistry()

    @JvmStatic
    fun main(args: Array<String>) {
        val host = System.getenv("HTTP_HOST") ?: "0.0.0.0"
        val port = System.getenv("HTTP_PORT")?.toIntOrNull() ?: 8080
        PekoraFramework.start(
            systemName = "AgentFramework",
            options = PekoraFrameworkOptions(
                nativeAgents = nativeAgents,
                plugins = listOf(FrameworkHttpPlugin(host, port)),
            ),
        )
    }
}
