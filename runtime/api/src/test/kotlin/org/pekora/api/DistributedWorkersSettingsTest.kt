package org.pekora.api

import com.typesafe.config.ConfigFactory
import org.junit.jupiter.api.Test
import org.pekora.framework.DistributedWorkerProvider
import org.pekora.framework.DistributedWorkersSettings
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DistributedWorkersSettingsTest {

    @Test
    fun `parses enum provider and embedded worker replicas`() {
        val config = ConfigFactory.parseString(
            """
            pekora.distributedWorkers {
              enabled = true
              provider = pekko
              leaseTimeoutMs = 15000
              queueActorName = "distributed-work-queue"
              embeddedWorkers {
                enabled = true
                replicas = 3
                pollIntervalMs = 500
                maxClaimsPerPoll = 12
                workerIdPrefix = "pekora-worker"
              }
            }
            """.trimIndent()
        )

        val settings = DistributedWorkersSettings.fromConfig(config)

        assertTrue(settings.enabled)
        assertEquals(DistributedWorkerProvider.PEKKO, settings.provider)
        assertEquals(Duration.ofSeconds(15), settings.leaseTimeout)
        assertEquals("distributed-work-queue", settings.queueActorName)
        assertEquals(3, settings.embeddedWorkers.replicas)
        assertEquals("pekora-worker", settings.embeddedWorkers.workerIdPrefix)
    }

    @Test
    fun `supports legacy embeddedWorker keys as fallback`() {
        val config = ConfigFactory.parseString(
            """
            pekora.distributedWorkers {
              enabled = true
              provider = pekko
              embeddedWorker {
                enabled = false
                pollIntervalMs = 900
                maxClaimsPerPoll = 4
                workerId = "legacy-worker"
              }
            }
            """.trimIndent()
        )

        val settings = DistributedWorkersSettings.fromConfig(config)

        assertEquals(DistributedWorkerProvider.PEKKO, settings.provider)
        assertEquals(false, settings.embeddedWorkers.enabled)
        assertEquals(Duration.ofMillis(900), settings.embeddedWorkers.pollInterval)
        assertEquals(4, settings.embeddedWorkers.maxClaimsPerPoll)
        assertEquals("legacy-worker", settings.embeddedWorkers.workerIdPrefix)
        assertEquals(1, settings.embeddedWorkers.replicas)
    }
}
