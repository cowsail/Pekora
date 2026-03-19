val pekkoHttpVersion: String by project

dependencies {
    implementation(project(":sdk:dsl"))
    implementation("org.apache.pekko:pekko-http_3:$pekkoHttpVersion")
}
