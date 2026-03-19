# Quickstart — Running the Framework Locally

## Prerequisites

- JDK 21+
- No external services required (in-memory persistence, single-node cluster)

## 1. Build

```bash
./gradlew clean build
```

If you've made code changes, always use `clean build` to avoid stale class issues with the Gradle daemon.

## 2. Start the Server

```bash
./gradlew :runtime:api:run
```

You should see output like:

```
INFO  org.apache.pekko.cluster.Cluster -- Cluster Node [...] - Started up successfully
INFO  org.pekora.api.FrameworkServer -- WorkflowRegistry started
INFO  org.pekora.api.FrameworkServer -- ApprovalManager started
INFO  org.pekora.api.FrameworkServer -- StepExecutor started
INFO  org.pekora.api.FrameworkServer -- RunEntity cluster sharding initialized
INFO  org.pekora.api.FrameworkServer -- HTTP server bound to /[0:0:0:0:0:0:0:0]:8080
```

The server is now running on `http://localhost:8080`.

## 3. Register a Workflow Template

```bash
curl -s -X POST http://localhost:8080/workflow-templates \
  -H "Content-Type: application/json" \
  -d '{
    "id": "issue-to-pr",
    "name": "Issue to PR",
    "description": "Converts issues to pull requests",
    "owner": "platform-team"
  }'
```

Expected response:

```json
{"message":"Template registered: issue-to-pr","success":true}
```

## 4. List Templates

```bash
curl -s http://localhost:8080/workflow-templates | python3 -m json.tool
```

Expected response:

```json
{
    "templates": [
        {
            "createdAt": "2026-03-18T02:35:08.353263Z",
            "description": "Converts issues to pull requests",
            "id": "issue-to-pr",
            "name": "Issue to PR",
            "owner": "platform-team",
            "tenantId": ""
        }
    ]
}
```

## 5. Publish a Workflow Version

This publishes a simple result-only workflow:

```bash
curl -s -X POST http://localhost:8080/workflow-templates/issue-to-pr/versions \
  -H "Content-Type: application/json" \
  -d '{
    "version": 1,
    "workflowYaml": "workflow:\n  name: issue-to-pr\n  version: 1\n  steps:\n    - id: start\n      type: result\n      output:\n        status: done"
  }'
```

Expected response:

```json
{"message":"Version 1 published","success":true}
```

## 6. Start a Run

```bash
curl -s -X POST http://localhost:8080/runs \
  -H "Content-Type: application/json" \
  -d '{
    "templateId": "issue-to-pr",
    "version": 1,
    "inputs": {
      "repo": "myorg/myrepo",
      "issue_id": "42"
    }
  }'
```

Expected response (your run ID will differ):

```json
{"message":"Run started","runId":"run_2b24b7aa-2812-45cc-a3bf-21bf71bd369e"}
```

Save the `runId` value — you'll need it for the next steps.

## 7. Check Run Status

Replace `{runId}` with the run ID from step 6:

```bash
curl -s http://localhost:8080/runs/{runId} | python3 -m json.tool
```

Expected response:

```json
{
    "error": null,
    "outputs": {},
    "runId": "run_2b24b7aa-2812-45cc-a3bf-21bf71bd369e",
    "status": "EXECUTING",
    "stepStates": {
        "start": "SUCCEEDED"
    }
}
```

## 8. Cancel a Run

```bash
curl -s -X POST http://localhost:8080/runs/{runId}/cancel
```

Expected response:

```json
{"message":"Run cancelled","success":true}
```

## 9. Test an Approval Workflow

Register and publish a workflow that includes an approval step:

```bash
# Register template
curl -s -X POST http://localhost:8080/workflow-templates \
  -H "Content-Type: application/json" \
  -d '{"id":"approval-test","name":"Approval Test"}'

# Publish version with approval step
curl -s -X POST http://localhost:8080/workflow-templates/approval-test/versions \
  -H "Content-Type: application/json" \
  -d '{
    "version": 1,
    "workflowYaml": "workflow:\n  name: approval-test\n  version: 1\n  steps:\n    - id: review\n      type: approval\n      approvers: [team-lead]\n      next: done\n    - id: done\n      type: result\n      output:\n        status: approved"
  }'

# Start the run
curl -s -X POST http://localhost:8080/runs \
  -H "Content-Type: application/json" \
  -d '{"templateId":"approval-test","version":1,"inputs":{}}'
```

## 10. Check Pending Approvals

```bash
curl -s http://localhost:8080/approvals | python3 -m json.tool
```

## 11. Approve or Reject

Replace `{approvalId}` with the approval ID from the pending list:

```bash
# Approve
curl -s -X POST http://localhost:8080/approvals/{approvalId}/approve \
  -H "Content-Type: application/json" \
  -d '{"approver":"team-lead","reason":"Looks good"}'

# Or reject
curl -s -X POST http://localhost:8080/approvals/{approvalId}/reject \
  -H "Content-Type: application/json" \
  -d '{"approver":"team-lead","reason":"Needs changes"}'
```

## 12. Resume a Paused Run

```bash
curl -s -X POST http://localhost:8080/runs/{runId}/resume
```

---

## All API Endpoints at a Glance

| Method | Path                                      | Description                  |
|--------|-------------------------------------------|------------------------------|
| POST   | `/workflow-templates`                     | Register a new template      |
| GET    | `/workflow-templates`                     | List all templates           |
| POST   | `/workflow-templates/{id}/versions`       | Publish a workflow version   |
| GET    | `/workflow-versions/{templateId}:{version}` | Get a specific version     |
| POST   | `/runs`                                   | Create and start a run       |
| GET    | `/runs/{runId}`                           | Get run status               |
| POST   | `/runs/{runId}/cancel`                    | Cancel a run                 |
| POST   | `/runs/{runId}/resume`                    | Resume a paused run          |
| GET    | `/approvals`                              | List pending approvals       |
| POST   | `/approvals/{approvalId}/approve`         | Approve a checkpoint         |
| POST   | `/approvals/{approvalId}/reject`          | Reject a checkpoint          |

---

## Stopping the Server

If you started the server in the foreground with `./gradlew :runtime:api:run`, press `Ctrl+C` to stop it.

If the process is backgrounded or the terminal was closed, kill it by port:

```bash
# Find and kill processes on the HTTP port (8080) and Pekko cluster port (25520)
lsof -ti:8080 -ti:25520 | xargs kill -9
```

You can also stop all Gradle daemons (which clears any cached class state):

```bash
./gradlew --stop
```

To verify nothing is still running:

```bash
lsof -i:8080
lsof -i:25520
```

Both should return no output if the server is fully stopped.

---

## Notes

- **In-memory persistence**: All state is lost when the server stops. This is fine for testing.
- **Single-node cluster**: The server runs a single-node Pekko cluster. No external coordination needed.
- **No external adapters**: Steps that reference `langgraph` or `openclaw` backends will fail because there is no backend service running. Result-only and approval workflows work end-to-end.
- **Port 25520**: Pekko cluster uses port 25520 for internal communication. If that port is in use, edit `runtime/run-engine/src/main/resources/application.conf`.
- **Port 8080**: The HTTP API runs on port 8080. Set `HTTP_PORT` environment variable to change it.
