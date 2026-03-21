val pekkoVersion: String by project
val pekkoHttpVersion: String by project

dependencies {
    implementation(project(":adapters:common"))
    implementation(project(":adapters:generic"))
    implementation(project(":sdk:dsl"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")

    testImplementation("org.apache.pekko:pekko-actor-testkit-typed_3:$pekkoVersion")
}
