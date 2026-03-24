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
    implementation(project(":runtime:framework"))
    implementation(project(":runtime:run-engine"))
    implementation(project(":runtime:projection"))
    implementation(project(":runtime:workflow-registry"))
    implementation(project(":adapters:common"))
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

    testImplementation("org.apache.pekko:pekko-actor-testkit-typed_3:$pekkoVersion")
    testImplementation("org.apache.pekko:pekko-http-testkit_3:$pekkoHttpVersion")
    testImplementation(project(":adapters:common"))
    testImplementation(project(":runtime:run-engine"))
    testImplementation(project(":runtime:projection"))
    testImplementation(project(":runtime:work-dispatch-core"))
    testImplementation(project(":runtime:workflow-registry"))
    testImplementation(project(":runtime:policy"))
}
