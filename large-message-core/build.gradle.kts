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



dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)

    implementation(group = "org.slf4j", name = "slf4j-api", version = "2.0.16")
    val awsVersion = "2.30.16"
    api(group = "software.amazon.awssdk", name = "s3", version = awsVersion)
    api(group = "software.amazon.awssdk", name = "sts", version = awsVersion)
    api(group = "com.azure", name = "azure-storage-blob", version = "12.29.0")
    api(group = "com.google.cloud", name = "google-cloud-storage", version = "2.46.0")
    implementation(group = "com.google.guava", name = "guava", version = "33.4.0-jre")

    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)
    val mockitoVersion = "5.15.2"
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    testImplementation(group = "com.google.cloud", name = "google-cloud-nio", version = "0.127.28")
    val testContainersVersion: String by project
    testFixturesApi(group = "org.testcontainers", name = "junit-jupiter", version = testContainersVersion)
    testFixturesImplementation(group = "org.testcontainers", name = "localstack", version = testContainersVersion)
}
