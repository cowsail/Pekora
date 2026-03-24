val pekkoVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation(project(":adapters:common"))
    implementation(project(":runtime:run-engine"))
    implementation(project(":runtime:work-dispatch-core"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-cluster-sharding-typed_3:$pekkoVersion")

    testImplementation("org.apache.pekko:pekko-actor-testkit-typed_3:$pekkoVersion")
}
