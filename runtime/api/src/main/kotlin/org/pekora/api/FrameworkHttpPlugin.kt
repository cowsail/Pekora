package org.pekora.api

import org.apache.pekko.http.javadsl.Http
import org.apache.pekko.http.javadsl.server.AllDirectives
import org.pekora.framework.PekoraFrameworkRuntime
import org.pekora.framework.PekoraPlugin
import org.slf4j.LoggerFactory

class FrameworkHttpPlugin(
    private val host: String = "0.0.0.0",
    private val port: Int = 8080,
) : PekoraPlugin {
    private val logger = LoggerFactory.getLogger(FrameworkHttpPlugin::class.java)

    override fun install(runtime: PekoraFrameworkRuntime) {
        val directives = object : AllDirectives() {}
        val route = directives.concat(
            WorkflowRoutes(runtime.registry, runtime.system).routes(),
            RunRoutes(
                runtime.sharding,
                runtime.registry,
                runtime.approvalManager,
                runtime.runProjection,
                runtime.system,
            ).routes(),
            HealthRoutes(runtime.agentAdapters, runtime.system).routes(),
        )

        Http.get(runtime.system).newServerAt(host, port).bind(route)
            .whenComplete { binding, error ->
                if (error != null) {
                    logger.error("Failed to bind HTTP server", error)
                    runtime.system.terminate()
                } else {
                    logger.info("HTTP server bound to {}", binding.localAddress())
                }
            }
    }
}
