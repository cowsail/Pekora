val pekkoVersion: String by project
val jacksonVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation(project(":adapters:common"))
    implementation(project(":runtime:policy"))
    implementation(project(":runtime:workflow-registry"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-cluster-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-cluster-sharding-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-persistence-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-serialization-jackson_3:$pekkoVersion")

    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")

    testImplementation("org.apache.pekko:pekko-actor-testkit-typed_3:$pekkoVersion")
    testImplementation("org.apache.pekko:pekko-persistence-testkit_3:$pekkoVersion")
}
