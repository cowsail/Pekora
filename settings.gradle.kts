rootProject.name = "pekora"

include(
    "runtime:api",
    "runtime:run-engine",
    "runtime:workflow-registry",
    "runtime:policy",
    "runtime:projection",
    "adapters:common",
    "adapters:langgraph",
    "adapters:openclaw-tools",
    "adapters:openclaw-skills",
    "sdk:dsl",
    "sdk:client",
)
