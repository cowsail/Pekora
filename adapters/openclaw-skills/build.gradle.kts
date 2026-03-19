val pekkoVersion: String by project
val pekkoHttpVersion: String by project

dependencies {
    implementation(project(":adapters:common"))
    implementation(project(":sdk:dsl"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-http_3:$pekkoHttpVersion")
}
