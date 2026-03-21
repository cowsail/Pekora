/**
 * Local workspace adapter for the Pekora framework.
 *
 * Implements [WorkspaceAdapter] for local filesystem-based workspaces:
 * - `"git-checkout"` type: clones a git repository into a temporary directory using
 *   `git clone --depth 1`.
 * - Other types: creates an empty temporary directory.
 *
 * Workspaces are cleaned up by deleting the temporary directory recursively.
 *
 * Phase 2 scope: instantiated in [org.pekora.api.FrameworkServer] for future use.
 * Automatic StepExecutor integration is deferred to Phase 3.
 *
 * @see WorkspaceAdapter
 */
package org.pekora.adapters

import org.slf4j.LoggerFactory
import java.io.File
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

/**
 * Workspace adapter that provisions and cleans up local filesystem directories.
 *
 * @property baseDir Parent directory for temporary workspaces. Defaults to the system temp dir.
 */
class LocalWorkspaceAdapter(
    private val baseDir: File = File(System.getProperty("java.io.tmpdir")),
) : WorkspaceAdapter {

    private val logger = LoggerFactory.getLogger(LocalWorkspaceAdapter::class.java)
    private val activeWorkspaces = mutableMapOf<String, File>()

    override fun prepare(request: WorkspaceRequest): CompletionStage<WorkspaceHandle> {
        return CompletableFuture.supplyAsync {
            val handleId = "ws_${request.runId}_${request.stepId}_${UUID.randomUUID()}"
            val workspaceDir = File(baseDir, handleId)
            workspaceDir.mkdirs()

            if (request.workspaceType == "git-checkout") {
                val repoUrl = request.config["repo_url"]
                    ?: throw IllegalArgumentException("git-checkout workspace requires config.repo_url")
                val branch = request.config["branch"] ?: "HEAD"
                gitClone(repoUrl, branch, workspaceDir)
            }

            synchronized(activeWorkspaces) {
                activeWorkspaces[handleId] = workspaceDir
            }
            logger.info("Workspace $handleId prepared at ${workspaceDir.absolutePath}")

            WorkspaceHandle(
                handleId = handleId,
                workspacePath = workspaceDir.absolutePath,
                metadata = mapOf("type" to request.workspaceType),
            )
        }
    }

    override fun cleanup(handleId: String): CompletionStage<Unit> {
        return CompletableFuture.supplyAsync {
            val dir = synchronized(activeWorkspaces) { activeWorkspaces.remove(handleId) }
            if (dir != null && dir.exists()) {
                dir.deleteRecursively()
                logger.info("Workspace $handleId cleaned up")
            } else {
                logger.warn("Workspace $handleId not found for cleanup")
            }
        }
    }

    private fun gitClone(repoUrl: String, branch: String, targetDir: File) {
        val process = ProcessBuilder(
            "git", "clone", "--depth", "1", "--branch", branch, repoUrl, targetDir.absolutePath,
        )
            .redirectErrorStream(true)
            .start()

        val exitCode = process.waitFor()
        if (exitCode != 0) {
            val output = process.inputStream.bufferedReader().readText()
            throw RuntimeException("git clone failed (exit $exitCode): $output")
        }
    }
}
