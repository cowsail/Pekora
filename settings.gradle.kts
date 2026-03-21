rootProject.name = "pekora"

include(
    "runtime:api",
    "runtime:run-engine",
    "runtime:workflow-registry",
    "runtime:policy",
    "runtime:projection",
    "adapters:common",
    "adapters:langgraph",
    "adapters:generic",
    "adapters:bedrock-agentcore",
    "adapters:a2a",
    "adapters:native",
    "sdk:dsl",
    "sdk:client",
)
