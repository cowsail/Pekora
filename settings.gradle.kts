rootProject.name = "pekora"

include(
    "runtime:api",
    "runtime:framework",
    "runtime:run-engine",
    "runtime:work-dispatch-core",
    "runtime:work-dispatch-pekko",
    "runtime:worker-runtime",
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
