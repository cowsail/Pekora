val pekkoVersion: String by project
val pekkoHttpVersion: String by project
val awsSdkVersion: String by project

dependencies {
    implementation(project(":adapters:common"))
    implementation(project(":sdk:dsl"))
    implementation("org.apache.pekko:pekko-actor-typed_3:$pekkoVersion")
    implementation("org.apache.pekko:pekko-http_3:$pekkoHttpVersion")
    implementation(platform("software.amazon.awssdk:bom:$awsSdkVersion"))
    implementation("software.amazon.awssdk:auth")
    implementation("software.amazon.awssdk:regions")
}
