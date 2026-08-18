description = "Large message client for Azure Blob Storage"

plugins {
    id("java-library")
}

dependencies {
    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.clients)

    api(project(":large-message-core"))
    api(libs.azure.storage.blob)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(platform(libs.kafka.bom))
    testImplementation(libs.kafka.clients)

    testImplementation(libs.log4j.slf4j2)
    testImplementation(libs.guava)
    testImplementation(testFixtures(project(":large-message-core")))
    testImplementation(libs.testcontainers.azure)
    testImplementation(libs.testcontainers.junit)
}
