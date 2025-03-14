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

description = "Kafka Connect converter that stores large messages on a blob storage, such as Amazon S3 and Azure Blob Storage"

plugins {
    id("java-library")
}


dependencies {
    api(project(":large-message-core"))
    api(group = "org.apache.kafka", name = "connect-api")
    compileOnly(group = "org.apache.kafka", name = "connect-runtime")

    val junitVersion: String by project
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testImplementation(project(":large-message-serde"))
    val assertJVersion: String by project
    testImplementation(group = "org.assertj", name = "assertj-core", version = assertJVersion)

    val log4jVersion: String by project
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j2-impl", version = log4jVersion)
    testImplementation(group = "org.apache.kafka", name = "connect-file")
    testImplementation(testFixtures(project(":large-message-core")))
    testImplementation(group = "org.apache.kafka", name = "connect-runtime")
    testImplementation(
        group = "org.apache.kafka",
        name = "connect-runtime",
        classifier = "test"
    )
    testImplementation(group = "org.apache.kafka", name = "kafka-clients", classifier = "test")
    testImplementation(group = "org.apache.kafka", name = "kafka_2.13")
    testImplementation(group = "org.apache.kafka", name = "kafka_2.13", classifier = "test")
}
