description = "Large message client for Google Cloud Storage"

plugins {
    id("java-library")
}

dependencies {
    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.clients)

    api(project(":large-message-core"))
    api(libs.google.cloud.storage)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(platform(libs.kafka.bom))
    testImplementation(libs.kafka.clients)

    testImplementation(libs.log4j.slf4j2)
    testImplementation(libs.google.cloud.nio)
}
