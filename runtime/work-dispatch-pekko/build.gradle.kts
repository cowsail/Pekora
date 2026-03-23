val pekkoVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation(project(":runtime:work-dispatch-core"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    testImplementation("org.apache.pekko:pekko-actor-testkit-typed_3:$pekkoVersion")
}
