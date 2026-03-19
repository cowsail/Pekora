package org.pekora.dsl

import org.yaml.snakeyaml.Yaml

/**
 * Parses workflow YAML into a [WorkflowDefinition].
 *
 * This parser implements the workflow DSL described in the design spec (Section 7).
 * It accepts YAML with a top-level `workflow` key and produces a fully-typed
 * [WorkflowDefinition] with all agents, tools, skills, policies, and steps resolved.
 *
 * ## Usage
 *
 * ```kotlin
 * val yaml = """
 *   workflow:
 *     name: my-workflow
 *     version: 1
 *     steps:
 *       - id: start
 *         type: result
 *         output:
 *           status: done
 * """.trimIndent()
 *
 * val definition = WorkflowParser.parse(yaml)
 * println(definition.name) // "my-workflow"
 * ```
 *
 * ## YAML Structure
 *
 * The parser expects the following top-level structure:
 * ```yaml
 * workflow:
 *   name: <string>           # required
 *   version: <int>           # defaults to 1
 *   description: <string>    # optional
 *   inputs: <schema>         # optional
 *   outputs: <schema>        # optional
 *   agents: [<agent>]        # optional
 *   tools: [<tool>]          # optional
 *   skills: [<skill>]        # optional
 *   policies: [<policy>]     # optional
 *   steps: [<step>]          # required
 * ```
 *
 * @throws IllegalArgumentException if the YAML is missing required fields or has an invalid structure.
 * @see WorkflowDefinition
 */
object WorkflowParser {

    /**
     * Parses a YAML string into a [WorkflowDefinition].
     *
     * @param yamlContent The raw YAML string containing a `workflow` top-level key.
     * @return A fully parsed [WorkflowDefinition].
     * @throws IllegalArgumentException if the YAML structure is invalid or required fields are missing.
     */
    fun parse(yamlContent: String): WorkflowDefinition {
        val yaml = Yaml()
        val raw = yaml.load<Map<String, Any>>(yamlContent)
        val wf = raw["workflow"] as? Map<*, *>
            ?: throw IllegalArgumentException("YAML must have a top-level 'workflow' key")
        return parseWorkflow(wf)
    }

    /**
     * Parses the `workflow` map into a [WorkflowDefinition].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseWorkflow(wf: Map<*, *>): WorkflowDefinition {
        val name = wf["name"] as? String ?: throw IllegalArgumentException("workflow.name is required")
        val version = (wf["version"] as? Number)?.toInt() ?: 1
        val description = wf["description"] as? String ?: ""

        val inputs = (wf["inputs"] as? Map<*, *>)?.let { parseSchema(it) }
        val outputs = (wf["outputs"] as? Map<*, *>)?.let { parseSchema(it) }

        val agents = (wf["agents"] as? List<*>)?.map { parseAgent(it as Map<*, *>) } ?: emptyList()
        val tools = (wf["tools"] as? List<*>)?.map { parseToolRef(it as Map<*, *>) } ?: emptyList()
        val skills = (wf["skills"] as? List<*>)?.map { parseSkillRef(it as Map<*, *>) } ?: emptyList()
        val policies = (wf["policies"] as? List<*>)?.map { parsePolicyRef(it as Map<*, *>) } ?: emptyList()
        val steps = (wf["steps"] as? List<*>)?.map { parseStep(it as Map<*, *>) }
            ?: throw IllegalArgumentException("workflow.steps is required")

        return WorkflowDefinition(
            name = name,
            version = version,
            description = description,
            inputs = inputs,
            outputs = outputs,
            agents = agents,
            tools = tools,
            skills = skills,
            policies = policies,
            steps = steps,
        )
    }

    /**
     * Parses a schema definition from a YAML map.
     *
     * @param map The raw YAML map for a schema block.
     * @return A [SchemaDefinition] with type, required fields, and property schemas.
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseSchema(map: Map<*, *>): SchemaDefinition {
        return SchemaDefinition(
            type = map["type"] as? String ?: "object",
            required = (map["required"] as? List<*>)?.map { it.toString() } ?: emptyList(),
            properties = (map["properties"] as? Map<*, *>)?.entries?.associate { (k, v) ->
                k.toString() to parsePropertySchema(v as Map<*, *>)
            } ?: emptyMap(),
        )
    }

    /**
     * Parses an individual property schema from a YAML map.
     */
    private fun parsePropertySchema(map: Map<*, *>): PropertySchema {
        return PropertySchema(
            type = map["type"] as? String ?: "string",
            description = map["description"] as? String ?: "",
            default = map["default"]?.toString(),
        )
    }

    /**
     * Parses an agent definition from a YAML map.
     *
     * @param map The raw YAML map for an agent entry.
     * @return An [AgentDefinition] with ID, backend, model profile, and config.
     * @throws IllegalArgumentException if `agent.id` is missing.
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAgent(map: Map<*, *>): AgentDefinition {
        return AgentDefinition(
            id = map["id"] as? String ?: throw IllegalArgumentException("agent.id is required"),
            backend = map["backend"] as? String ?: "native",
            modelProfile = map["model_profile"] as? String ?: "default",
            description = map["description"] as? String ?: "",
            config = (map["config"] as? Map<*, *>)?.entries?.associate { (k, v) -> k.toString() to v.toString() }
                ?: emptyMap(),
        )
    }

    /**
     * Parses a tool reference from a YAML map.
     *
     * @throws IllegalArgumentException if `tool.id` is missing.
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseToolRef(map: Map<*, *>): ToolReference {
        return ToolReference(
            id = map["id"] as? String ?: throw IllegalArgumentException("tool.id is required"),
            adapter = map["adapter"] as? String ?: "native",
            config = (map["config"] as? Map<*, *>)?.entries?.associate { (k, v) -> k.toString() to v.toString() }
                ?: emptyMap(),
        )
    }

    /**
     * Parses a skill reference from a YAML map.
     *
     * @throws IllegalArgumentException if `skill.id` is missing.
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseSkillRef(map: Map<*, *>): SkillReference {
        return SkillReference(
            id = map["id"] as? String ?: throw IllegalArgumentException("skill.id is required"),
            adapter = map["adapter"] as? String ?: "native",
            config = (map["config"] as? Map<*, *>)?.entries?.associate { (k, v) -> k.toString() to v.toString() }
                ?: emptyMap(),
        )
    }

    /**
     * Parses a policy reference from a YAML map, including optional inline policy definition.
     */
    @Suppress("UNCHECKED_CAST")
    private fun parsePolicyRef(map: Map<*, *>): PolicyReference {
        return PolicyReference(
            id = map["id"] as? String ?: "",
            inline = (map["inline"] as? Map<*, *>)?.let { parsePolicyDef(it) },
        )
    }

    /**
     * Parses an inline policy definition from a YAML map.
     */
    @Suppress("UNCHECKED_CAST")
    private fun parsePolicyDef(map: Map<*, *>): PolicyDefinition {
        return PolicyDefinition(
            id = map["id"] as? String ?: "",
            allowedBackends = (map["allowed_backends"] as? List<*>)?.map { it.toString() } ?: emptyList(),
            allowedModels = (map["allowed_models"] as? List<*>)?.map { it.toString() } ?: emptyList(),
            allowedTools = (map["allowed_tools"] as? List<*>)?.map { it.toString() } ?: emptyList(),
            allowedSkills = (map["allowed_skills"] as? List<*>)?.map { it.toString() } ?: emptyList(),
            maxTokens = (map["max_tokens"] as? Number)?.toLong(),
            timeoutSeconds = (map["timeout_seconds"] as? Number)?.toInt(),
            requireApproval = map["require_approval"] as? Boolean ?: false,
        )
    }

    /**
     * Parses a step definition from a YAML map.
     *
     * Resolves the [StepKind] from the `type` string, and extracts all optional fields
     * relevant to the step kind (agent, tool, skill, branches, retries, etc.).
     *
     * @param map The raw YAML map for a step entry.
     * @return A [StepDefinition] with all fields populated.
     * @throws IllegalArgumentException if `step.id` or `step.type` is missing or invalid.
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseStep(map: Map<*, *>): StepDefinition {
        val typeStr = map["type"] as? String ?: throw IllegalArgumentException("step.type is required")
        val type = StepKind.entries.find { it.name.equals(typeStr, ignoreCase = true) }
            ?: throw IllegalArgumentException("Unknown step type: $typeStr")

        return StepDefinition(
            id = map["id"] as? String ?: throw IllegalArgumentException("step.id is required"),
            type = type,
            agent = map["agent"] as? String,
            tool = map["tool"] as? String,
            skill = map["skill"] as? String,
            input = (map["input"] as? Map<*, *>)?.entries?.associate { (k, v) -> k.toString() to v.toString() }
                ?: emptyMap(),
            output = (map["output"] as? Map<*, *>)?.entries?.associate { (k, v) -> k.toString() to v.toString() }
                ?: emptyMap(),
            next = map["next"] as? String,
            branches = (map["branches"] as? List<*>)?.map { parseBranch(it as Map<*, *>) } ?: emptyList(),
            parallel = (map["parallel"] as? List<*>)?.map { it.toString() } ?: emptyList(),
            joinNext = map["join_next"] as? String,
            subworkflow = map["subworkflow"] as? String,
            approvers = (map["approvers"] as? List<*>)?.map { it.toString() } ?: emptyList(),
            timeout = (map["timeout"] as? Number)?.toInt(),
            retries = (map["retries"] as? Map<*, *>)?.let { parseRetryConfig(it) },
            policy = map["policy"] as? String,
            description = map["description"] as? String ?: "",
        )
    }

    /**
     * Parses a branch definition for decision steps.
     */
    private fun parseBranch(map: Map<*, *>): BranchDefinition {
        return BranchDefinition(
            condition = map["condition"] as? String ?: "",
            next = map["next"] as? String ?: "",
        )
    }

    /**
     * Parses a retry configuration block.
     */
    private fun parseRetryConfig(map: Map<*, *>): RetryConfig {
        return RetryConfig(
            maxAttempts = (map["max_attempts"] as? Number)?.toInt() ?: 3,
            backoffMs = (map["backoff_ms"] as? Number)?.toLong() ?: 1000,
            multiplier = (map["multiplier"] as? Number)?.toDouble() ?: 2.0,
        )
    }
}
