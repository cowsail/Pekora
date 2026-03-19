val pekkoVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
}
