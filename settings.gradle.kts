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
    "adapters:strands",
    "adapters:openclaw",
    "sdk:dsl",
    "sdk:client",
)
