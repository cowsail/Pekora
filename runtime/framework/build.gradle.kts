val pekkoVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation(project(":runtime:run-engine"))
    implementation(project(":runtime:projection"))
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

    testImplementation("org.apache.pekko:pekko-actor-testkit-typed_3:$pekkoVersion")
}
