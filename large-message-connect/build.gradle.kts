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
    api(libs.kafka.connect.api)
    compileOnly(libs.kafka.connect.runtime)

    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.junit.jupiter)
    testImplementation(project(":large-message-serde"))
    testImplementation(libs.assertj)

    testImplementation(libs.log4j.slf4j2)
    testImplementation(libs.kafka.connect.file)
    testImplementation(testFixtures(project(":large-message-core")))
    testImplementation(libs.kafka.connect.runtime)
    testImplementation(variantOf(libs.kafka.connect.runtime) {
        classifier("test")
    })
    testImplementation(variantOf(libs.kafka.clients) {
        classifier("test")
    })
    testImplementation(libs.kafka.core)
    testImplementation(variantOf(libs.kafka.core) {
        classifier("test")
    })
    testImplementation(variantOf(libs.kafka.server.common) {
        classifier("test")
    })
}
