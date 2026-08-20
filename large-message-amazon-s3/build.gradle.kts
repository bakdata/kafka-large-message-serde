description = "Large message client for Amazon S3"

plugins {
    id("java-library")
}

dependencies {
    compileOnly(platform(libs.kafka.bom))
    compileOnly(libs.kafka.clients)

    api(project(":large-message-core"))
    api(libs.aws.s3)
    api(libs.aws.sts)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(platform(libs.kafka.bom))
    testImplementation(libs.kafka.clients)

    testImplementation(libs.log4j.slf4j2)
    testImplementation(libs.guava)
    testImplementation(testFixtures(project(":large-message-core")))
    testFixturesApi(libs.testcontainers.junit)
    testFixturesImplementation(libs.testcontainers.localstack)
}
