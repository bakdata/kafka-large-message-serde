/*
 * MIT License
 *
 * Copyright (c) 2022 bakdata
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

description = "Base module for Kafka plugins that store large messages on a blob storage, such as Amazon S3 and Azure Blob Storage"

plugins {
    id("java-library")
}

configurations.all {
    exclude(group = "commons-logging", module = "commons-logging")
}

dependencies {
    implementation(platform(libs.kafka.bom))
    implementation(libs.slf4j.api)
    implementation(libs.slf4j.jcl)
    implementation(libs.guava)
    api(libs.aws.s3)
    api(libs.aws.sts)
    api(libs.azure.storage.blob)
    api(libs.google.cloud.storage)
    compileOnly(libs.kafka.clients)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit)
    testImplementation(libs.kafka.clients)

    testImplementation(libs.log4j.slf4j2)
    testImplementation(libs.google.cloud.nio)
    testFixturesApi(libs.testcontainers.junit)
    testFixturesImplementation(libs.testcontainers.localstack)
}
