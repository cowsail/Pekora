# Adapter Configuration

This document describes how to configure the four built-in agent runtime adapters in Pekora.

## HOCON Configuration

All adapters are configured under the `pekora.adapters` block in `application.conf`. Each adapter can be disabled by setting `enabled = false`.

```hocon
pekora {
  adapters {
    langgraph {
      enabled = true
      service-url = "http://localhost:8123"
      api-key = ""
    }
    a2a {
      enabled = true
      service-url = "http://localhost:8200"
      api-key = ""
    }
    bedrock-agentcore {
      enabled = true
      agent-runtime-arn = ""
      region = "us-east-1"
      auth-mode = "sigv4"   # "sigv4" or "oauth"
      oauth-token = ""
    }
    generic {
      enabled = true
      service-url = "http://localhost:8400"
      api-key = ""
    }
  }
}
```

## Environment Variable Overrides

Every config value has a corresponding environment variable override:

| Adapter           | Config Key                       | Environment Variable          |
|-------------------|----------------------------------|-------------------------------|
| LangGraph         | `service-url`                    | `LANGGRAPH_SERVICE_URL`       |
| LangGraph         | `api-key`                        | `LANGGRAPH_API_KEY`           |
| A2A               | `service-url`                    | `A2A_SERVICE_URL`             |
| A2A               | `api-key`                        | `A2A_API_KEY`                 |
| BedrockAgentCore  | `agent-runtime-arn`              | `BEDROCK_AGENT_RUNTIME_ARN`   |
| BedrockAgentCore  | `region`                         | `AWS_REGION`                  |
| BedrockAgentCore  | `auth-mode`                      | `BEDROCK_AUTH_MODE`           |
| BedrockAgentCore  | `oauth-token`                    | `BEDROCK_OAUTH_TOKEN`         |
| Generic           | `service-url`                    | `GENERIC_SERVICE_URL`         |
| Generic           | `api-key`                        | `GENERIC_API_KEY`             |

## Adapter Details

### LangGraph (`backend: langgraph`)

Connects to a [LangGraph AgentServer](https://langchain-ai.github.io/langgraph/cloud/) instance.

**API contract (thread + run model):**
- `POST /threads` — create a new conversation thread
- `POST /threads/{thread_id}/runs` — start a run with `assistant_id` and `input`
- `GET /threads/{thread_id}/runs/{run_id}` — poll run status until terminal
- `GET /threads/{thread_id}` — retrieve final thread state on success

**Auth:** `X-Api-Key: <api-key>` header on all requests.

**Workflow DSL example:**
```yaml
agents:
  - id: planner
    backend: langgraph
    config:
      graph_id: issue-to-pr-graph
```

---

### A2A (`backend: a2a`)

Connects to any agent implementing the [A2A (Agent-to-Agent) protocol](https://google.github.io/A2A/) — a JSON-RPC 2.0 open interop standard.

**API contract:**
- `POST {service-url}` — JSON-RPC 2.0 method `message/send`
- `GET {service-url}/.well-known/agent-card.json` — agent discovery and health check

**Request format:**
```json
{
  "jsonrpc": "2.0",
  "id": "<correlationId>",
  "method": "message/send",
  "params": {
    "message": {
      "role": "user",
      "parts": [{"kind": "text", "text": "<serialized input>"}],
      "messageId": "<stepId>"
    }
  }
}
```

**Auth:** `Authorization: Bearer <api-key>` when api-key is non-empty.

**Workflow DSL example:**
```yaml
agents:
  - id: researcher
    backend: a2a
```

---

### BedrockAgentCore (`backend: bedrock-agentcore`)

Connects to agents deployed on [Amazon Bedrock AgentCore](https://aws.amazon.com/bedrock/agentcore/).

**API contract:**
- `POST /runtimes/{agentRuntimeArn}/invocations` — invoke the agent

**Auth modes:**

| Mode     | Description                                              |
|----------|----------------------------------------------------------|
| `sigv4`  | AWS Signature Version 4 via `DefaultCredentialsProvider` |
| `oauth`  | `Authorization: Bearer <oauth-token>` header             |

**SigV4 credentials** are resolved from the standard AWS credential chain (env vars, `~/.aws/credentials`, instance role, etc.). No additional configuration needed beyond `auth-mode = "sigv4"`.

**Workflow DSL example:**
```yaml
agents:
  - id: analyzer
    backend: bedrock-agentcore
```

---

### Generic (`backend: generic`)

Escape hatch for custom HTTP or in-process actor backends.

**HTTP mode:** POSTs a serialized `StepExecutionRequest` to `{service-url}/execute`. Expects a JSON response with `status`, `output`, and optional `error` fields.

**Actor mode:** Configured programmatically in `FrameworkServer` via `GenericAdapter.actor(...)`. The target actor must accept `GenericActorRequest` and reply with `StepExecutionResult`.

**Auth:** `Authorization: Bearer <api-key>` when api-key is non-empty.

**Workflow DSL example:**
```yaml
agents:
  - id: custom-tool
    backend: generic
```

---

## Health Check Endpoint

`GET /health/adapters` returns the health of all registered adapters:

```json
{
  "adapters": {
    "langgraph":          { "status": "HEALTHY",   "message": "", "latencyMs": 12 },
    "a2a":                { "status": "UNHEALTHY",  "message": "connection refused", "latencyMs": 0 },
    "bedrock-agentcore":  { "status": "UNKNOWN",   "message": "agent-runtime-arn not configured", "latencyMs": 0 },
    "generic":            { "status": "HEALTHY",   "message": "", "latencyMs": 3 }
  }
}
```

Status values: `HEALTHY`, `UNHEALTHY`, `UNKNOWN`.

---

## Registering a Custom Adapter

Implement `AgentRuntimeAdapter` from `adapters:common`:

```kotlin
class MyAdapter : AgentRuntimeAdapter {
    override val backendId = "my-backend"

    override fun executeStep(request: StepExecutionRequest): CompletionStage<StepExecutionResult> {
        // Map request to your backend, return normalized result
    }

    override fun healthCheck(): CompletionStage<AdapterHealth> {
        // Probe your backend, return AdapterHealth
    }
}
```

Register it in `AdapterFactory.createAdapters()` or pass it directly to `StepExecutor.create(agentAdapters = ...)` in `FrameworkServer`.

Reference your backend in the workflow YAML:
```yaml
agents:
  - id: my-agent
    backend: my-backend
```
