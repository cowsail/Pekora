val pekkoVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation(project(":runtime:policy"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-cluster-sharding-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-persistence-typed_3:$pekkoVersion")
}
