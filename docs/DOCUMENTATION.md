# Documentation Generation Runbook

This guide explains how to generate, view, and publish API documentation for the Pekko Agent Workflow Framework using [Dokka](https://kotlin.github.io/dokka/), Kotlin's documentation engine.

## Prerequisites

- JDK 21+
- The project builds successfully: `./gradlew clean build`

---

## Generating Documentation

### Generate HTML Documentation (All Modules — Aggregated)

```bash
./gradlew dokkaGeneratePublicationHtml
```

This generates a unified HTML site that aggregates all 11 submodules into a single browsable publication with cross-module linking.

**Output location**: `build/dokka/html/index.html`

### Generate HTML Documentation (Single Module)

To generate documentation for a specific module only:

```bash
# SDK DSL models and parser
./gradlew :sdk:dsl:dokkaGeneratePublicationHtml

# Adapter interfaces
./gradlew :adapters:common:dokkaGeneratePublicationHtml

# LangGraph adapter
./gradlew :adapters:langgraph:dokkaGeneratePublicationHtml

# OpenClaw tool adapter
./gradlew :adapters:openclaw-tools:dokkaGeneratePublicationHtml

# OpenClaw skill adapter
./gradlew :adapters:openclaw-skills:dokkaGeneratePublicationHtml

# Run engine (RunEntity, StepExecutor, ApprovalManager)
./gradlew :runtime:run-engine:dokkaGeneratePublicationHtml

# Workflow registry
./gradlew :runtime:workflow-registry:dokkaGeneratePublicationHtml

# Policy guard
./gradlew :runtime:policy:dokkaGeneratePublicationHtml

# Projections
./gradlew :runtime:projection:dokkaGeneratePublicationHtml

# HTTP API routes
./gradlew :runtime:api:dokkaGeneratePublicationHtml

# Client SDK
./gradlew :sdk:client:dokkaGeneratePublicationHtml
```

**Output location**: `<module>/build/dokka/html/index.html`

---

## Viewing Documentation

### Option 1: Open Directly in Browser

After generating, open the HTML index file in your browser:

```bash
# Aggregated multi-module docs
open build/dokka/html/index.html

# Single module (example: sdk/dsl)
open sdk/dsl/build/dokka/html/index.html
```

On Linux, use `xdg-open` instead of `open`.

### Option 2: Serve Locally with Python

For a better experience (proper CSS/JS loading), serve the docs with a local HTTP server:

```bash
cd build/dokka/html
python3 -m http.server 9090
```

Then open `http://localhost:9090` in your browser. Press `Ctrl+C` to stop.

### Option 3: Serve Locally with Node.js

```bash
npx serve build/dokka/html -p 9090
```

---

## Documentation Structure

The generated documentation is organized by module and package:

```
build/dokka/html/
├── index.html                          # Landing page with all modules
├── sdk/
│   ├── dsl/
│   │   └── org.pekora.dsl/         # DSL models, events, parser
│   │       ├── -workflow-definition/
│   │       ├── -step-definition/
│   │       ├── -run-event/
│   │       ├── -workflow-parser/
│   │       └── ...
│   └── client/
│       └── org.pekora.client/      # Client SDK
│           └── -framework-client/
├── adapters/
│   ├── common/
│   │   └── org.pekora.adapters/    # Adapter interfaces
│   ├── langgraph/
│   ├── openclaw-tools/
│   └── openclaw-skills/
└── runtime/
    ├── run-engine/
    │   └── org.pekora.engine/      # Core runtime
    │       ├── -run-entity/
    │       ├── -step-executor/
    │       ├── -approval-manager/
    │       └── ...
    ├── policy/
    ├── workflow-registry/
    ├── projection/
    └── api/
        └── org.pekora.api/         # HTTP API
```

## Key Pages to Read

| Page | What You'll Learn |
|------|-------------------|
| `WorkflowDefinition` | Full DSL model — how workflows are structured |
| `StepKind` | All supported step types and when to use each |
| `RunEvent` | Event sourcing model — every event type in the run journal |
| `RunState` / `StepState` | Lifecycle state machines for runs and steps |
| `AgentRuntimeAdapter` | How to implement a custom backend adapter |
| `PolicyGuard` | How policies are evaluated and composed |
| `RunEntity` | Core runtime — how the event-sourced actor works |
| `StepExecutor` | Step dispatching — how steps are routed to adapters |
| `FrameworkClient` | Client SDK — how to interact with the framework programmatically |

---

## Writing KDoc

All source files use [KDoc](https://kotlinlang.org/docs/kotlin-doc.html), Kotlin's documentation syntax. Here are the conventions used in this project:

### Class Documentation

```kotlin
/**
 * Brief description of the class.
 *
 * Longer description with context, design rationale, and usage examples.
 *
 * @property foo Description of the foo property.
 * @property bar Description of the bar property.
 * @see RelatedClass
 */
class MyClass(val foo: String, val bar: Int)
```

### Method Documentation

```kotlin
/**
 * Brief description of what the method does.
 *
 * @param input Description of the input parameter.
 * @return Description of the return value.
 * @throws IllegalArgumentException if input is invalid.
 * @see RelatedMethod
 */
fun doSomething(input: String): Result
```

### Package Documentation

Place a package-level KDoc comment at the top of the file, before the `package` statement:

```kotlin
/**
 * Description of what this package contains and its role in the framework.
 *
 * @see org.pekora.other.RelatedPackage
 */
package org.pekora.mypackage
```

### Linking to Other Types

Use bracket syntax to create links in KDoc:

```kotlin
/**
 * This class works with [WorkflowDefinition] and produces [StepExecutionResult].
 * See [org.pekora.engine.RunEntity] for the runtime execution.
 */
```

---

## CI/CD Integration

To integrate documentation generation into your CI pipeline:

```yaml
# Example GitHub Actions step
- name: Generate Documentation
  run: ./gradlew dokkaGeneratePublicationHtml

- name: Upload Documentation
  uses: actions/upload-artifact@v4
  with:
    name: api-docs
    path: build/dokka/html/
```

To publish to GitHub Pages:

```yaml
- name: Deploy to GitHub Pages
  uses: peaceiris/actions-gh-pages@v4
  with:
    github_token: ${{ secrets.GITHUB_TOKEN }}
    publish_dir: build/dokka/html
```

---

## Troubleshooting

### Build fails with "implicit dependency" Dokka error

This project uses **Dokka V2** mode. The following line must be present in `gradle.properties`:

```properties
org.jetbrains.dokka.experimental.gradle.pluginMode=V2EnabledWithHelpers
```

And the root `build.gradle.kts` must declare the aggregated submodules via `dokka(project(...))` dependencies:

```kotlin
dependencies {
    dokka(project(":sdk:dsl"))
    dokka(project(":adapters:common"))
    // ... all leaf modules
}
```

### Documentation is empty or missing modules

Run a clean regeneration:

```bash
./gradlew --stop
./gradlew clean dokkaGeneratePublicationHtml
```

### Documentation is missing for some classes

- Verify the source files have `public` visibility (Dokka skips `internal` and `private` by default)
- Check that the module's `build.gradle.kts` has the Dokka plugin applied (done automatically via the root `subprojects` block)

### Broken cross-module links

Use the root-level `dokkaGeneratePublicationHtml` (not a per-module task) to get working cross-module links, since aggregation is configured only at the root.
