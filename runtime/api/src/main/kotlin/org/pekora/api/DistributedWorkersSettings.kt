package org.pekora.api

import com.typesafe.config.Config
import java.time.Duration

enum class DistributedWorkerProvider {
    INLINE,
    PEKKO,
}

data class EmbeddedWorkersSettings(
    val enabled: Boolean,
    val replicas: Int,
    val pollInterval: Duration,
    val maxClaimsPerPoll: Int,
    val workerIdPrefix: String,
)

data class DistributedWorkersSettings(
    val enabled: Boolean,
    val provider: DistributedWorkerProvider,
    val leaseTimeout: Duration,
    val queueActorName: String,
    val embeddedWorkers: EmbeddedWorkersSettings,
) {
    companion object {
        fun fromConfig(config: Config): DistributedWorkersSettings {
            val rootPath = "pekora.distributedWorkers"
            val enabled = config.hasPath("$rootPath.enabled") && config.getBoolean("$rootPath.enabled")
            val provider = providerFromConfig(config, "$rootPath.provider")
            val leaseTimeout = Duration.ofMillis(
                if (config.hasPath("$rootPath.leaseTimeoutMs")) config.getLong("$rootPath.leaseTimeoutMs") else 60_000L
            )
            val queueActorName = if (config.hasPath("$rootPath.queueActorName")) {
                config.getString("$rootPath.queueActorName")
            } else {
                "work-queue"
            }

            return DistributedWorkersSettings(
                enabled = enabled,
                provider = provider,
                leaseTimeout = leaseTimeout,
                queueActorName = queueActorName,
                embeddedWorkers = EmbeddedWorkersSettings(
                    enabled = if (config.hasPath("$rootPath.embeddedWorkers.enabled")) {
                        config.getBoolean("$rootPath.embeddedWorkers.enabled")
                    } else if (config.hasPath("$rootPath.embeddedWorker.enabled")) {
                        config.getBoolean("$rootPath.embeddedWorker.enabled")
                    } else {
                        true
                    },
                    replicas = if (config.hasPath("$rootPath.embeddedWorkers.replicas")) {
                        config.getInt("$rootPath.embeddedWorkers.replicas")
                    } else {
                        1
                    },
                    pollInterval = Duration.ofMillis(
                        if (config.hasPath("$rootPath.embeddedWorkers.pollIntervalMs")) {
                            config.getLong("$rootPath.embeddedWorkers.pollIntervalMs")
                        } else if (config.hasPath("$rootPath.embeddedWorker.pollIntervalMs")) {
                            config.getLong("$rootPath.embeddedWorker.pollIntervalMs")
                        } else {
                            250L
                        }
                    ),
                    maxClaimsPerPoll = if (config.hasPath("$rootPath.embeddedWorkers.maxClaimsPerPoll")) {
                        config.getInt("$rootPath.embeddedWorkers.maxClaimsPerPoll")
                    } else if (config.hasPath("$rootPath.embeddedWorker.maxClaimsPerPoll")) {
                        config.getInt("$rootPath.embeddedWorker.maxClaimsPerPoll")
                    } else {
                        8
                    },
                    workerIdPrefix = if (config.hasPath("$rootPath.embeddedWorkers.workerIdPrefix")) {
                        config.getString("$rootPath.embeddedWorkers.workerIdPrefix")
                    } else if (config.hasPath("$rootPath.embeddedWorker.workerId")) {
                        config.getString("$rootPath.embeddedWorker.workerId")
                    } else {
                        "embedded-worker"
                    },
                ),
            )
        }

        private fun providerFromConfig(config: Config, path: String): DistributedWorkerProvider {
            if (!config.hasPath(path)) {
                return DistributedWorkerProvider.INLINE
            }
            return when (config.getString(path).trim().lowercase()) {
                "inline" -> DistributedWorkerProvider.INLINE
                "pekko" -> DistributedWorkerProvider.PEKKO
                else -> DistributedWorkerProvider.INLINE
            }
        }
    }
}
