/**
 * HTTP routes for adapter health checks.
 *
 * Exposes `GET /health/adapters` which queries every registered [AgentRuntimeAdapter]
 * concurrently and returns an aggregated JSON health report.
 *
 * Response shape:
 * ```json
 * {
 *   "adapters": {
 *     "langgraph": { "status": "HEALTHY", "message": "", "latencyMs": 12 },
 *     "a2a":       { "status": "UNHEALTHY", "message": "connection refused", "latencyMs": 0 }
 *   }
 * }
 * ```
 *
 * @see AdapterFactory
 * @see FrameworkServer
 */
package org.pekora.api

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.http.javadsl.marshallers.jackson.Jackson
import org.apache.pekko.http.javadsl.model.StatusCodes
import org.apache.pekko.http.javadsl.server.AllDirectives
import org.apache.pekko.http.javadsl.server.Route
import org.pekora.adapters.AdapterHealth
import org.pekora.adapters.AgentRuntimeAdapter
import org.pekora.adapters.HealthStatus
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture

/**
 * Defines the `GET /health/adapters` route.
 *
 * @param adapters Map of backend-id to adapter instances to probe.
 * @param system Actor system (used for Pekka HTTP route context).
 */
class HealthRoutes(
    private val adapters: Map<String, AgentRuntimeAdapter>,
    private val system: ActorSystem<*>,
) : AllDirectives() {

    private val logger = LoggerFactory.getLogger(HealthRoutes::class.java)
    private val mapper = ObjectMapper()

    fun routes(): Route = pathPrefix("health") {
        path("adapters") {
            get {
                val healthFutures = adapters.map { (id, adapter) ->
                    adapter.healthCheck()
                        .exceptionally { ex ->
                            logger.warn("Health check failed for adapter $id", ex)
                            AdapterHealth(id, HealthStatus.UNHEALTHY, ex.message ?: "health check failed")
                        }
                        .toCompletableFuture()
                        .thenApply { health -> id to health }
                }

                val allHealthFuture = CompletableFuture.allOf(*healthFutures.toTypedArray())
                    .thenApply {
                        val results = healthFutures.associate { it.join() }
                        buildHealthResponse(results)
                    }

                completeOKWithFuture(allHealthFuture, Jackson.marshaller(mapper))
            }
        }
    }

    private fun buildHealthResponse(results: Map<String, AdapterHealth>): Map<String, Any> {
        val adapterMap = results.mapValues { (_, health) ->
            mapOf(
                "status" to health.status.name,
                "message" to health.message,
                "latencyMs" to health.latencyMs,
            )
        }
        return mapOf("adapters" to adapterMap)
    }
}
