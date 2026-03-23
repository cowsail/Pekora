val pekkoVersion: String by project
val pekkoHttpVersion: String by project
val jacksonVersion: String by project
val logbackVersion: String by project

plugins {
    application
}

application {
    mainClass.set("org.pekora.api.FrameworkServer")
}

dependencies {
    implementation(project(":sdk:dsl"))
    implementation(project(":runtime:run-engine"))
    implementation(project(":runtime:work-dispatch-core"))
    implementation(project(":runtime:work-dispatch-pekko"))
    implementation(project(":runtime:worker-runtime"))
    implementation(project(":runtime:workflow-registry"))
    implementation(project(":runtime:policy"))
    implementation(project(":adapters:common"))
    implementation(project(":adapters:langgraph"))
    implementation(project(":adapters:a2a"))
    implementation(project(":adapters:bedrock-agentcore"))
    implementation(project(":adapters:generic"))
    implementation(project(":adapters:native"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-cluster-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-cluster-sharding-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-persistence-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-http_3:$pekkoHttpVersion")
    implementation("org.apache.pekko:pekko-http-jackson_3:$pekkoHttpVersion")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
}
