plugins {
    kotlin("jvm") version "2.0.21" apply false
    kotlin("plugin.serialization") version "2.0.21" apply false
    id("org.jetbrains.dokka") version "2.0.0"
}

// Dokka V2: aggregate all leaf modules into a single root-level HTML publication
dependencies {
    dokka(project(":sdk:dsl"))
    dokka(project(":sdk:client"))
    dokka(project(":adapters:common"))
    dokka(project(":adapters:langgraph"))
    dokka(project(":adapters:generic"))
    dokka(project(":adapters:strands"))
    dokka(project(":adapters:openclaw"))
    dokka(project(":runtime:run-engine"))
    dokka(project(":runtime:workflow-registry"))
    dokka(project(":runtime:policy"))
    dokka(project(":runtime:projection"))
    dokka(project(":runtime:api"))
}

allprojects {
    group = "org.pekkoagent"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    apply(plugin = "org.jetbrains.kotlin.plugin.serialization")
    apply(plugin = "org.jetbrains.dokka")

    val pekkoVersion: String by project
    val pekkoHttpVersion: String by project
    val jacksonVersion: String by project
    val kotlinxSerializationVersion: String by project
    val kotlinxCoroutinesVersion: String by project
    val slf4jVersion: String by project
    val logbackVersion: String by project
    val junitVersion: String by project

    dependencies {
        "implementation"(kotlin("stdlib"))
        "implementation"("org.jetbrains.kotlinx:kotlinx-coroutines-core:$kotlinxCoroutinesVersion")
        "implementation"("org.jetbrains.kotlinx:kotlinx-serialization-json:$kotlinxSerializationVersion")
        "implementation"("org.slf4j:slf4j-api:$slf4jVersion")

        "testImplementation"(kotlin("test"))
        "testImplementation"("org.junit.jupiter:junit-jupiter:$junitVersion")
        "testRuntimeOnly"("ch.qos.logback:logback-classic:$logbackVersion")
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        compilerOptions {
            jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
            freeCompilerArgs.add("-Xjsr305=strict")
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    configure<JavaPluginExtension> {
        sourceCompatibility = JavaVersion.VERSION_21
        targetCompatibility = JavaVersion.VERSION_21
    }
}
