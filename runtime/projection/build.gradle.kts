val pekkoVersion: String by project
val pekkoConnectorsVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation(project(":runtime:run-engine"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-persistence-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-persistence-query_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-projection-eventsourced_3:$pekkoConnectorsVersion")
}
