description = "Kafka Serde that stores large messages on Amazon S3"


plugins {
    `java-library`
    id("net.researchgate.release") version "2.6.0"
    id("com.bakdata.sonar") version "1.1.4"
    id("com.bakdata.sonatype") version "1.1.4"
    id("org.hildan.github.changelog") version "0.8.0"
    id("io.freefair.lombok") version "3.8.0"
}

group = "com.bakdata.kafka"

tasks.withType<Test> {
    maxParallelForks = 4
    useJUnitPlatform()
}

repositories {
    mavenCentral()
    maven(url = "http://packages.confluent.io/maven/")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Sven Lehmann")
            id.set("SvenLehmann")
        }
        developer {
            name.set("Torben Meyer")
            id.set("torbsto")
        }
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}

configure<org.hildan.github.changelog.plugin.GitHubChangelogExtension> {
    githubUser = "bakdata"
    futureVersionTag = findProperty("changelog.releaseVersion")?.toString()
    sinceTag = findProperty("changelog.sinceTag")?.toString()
}

dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka_2.11", version = kafkaVersion)

    val confluentVersion: String by project
    api(group = "io.confluent", name = "common-config", version = confluentVersion)

    api(group = "org.slf4j", name = "slf4j-api", version = "1.7.26")
    api(group = "com.amazonaws", name = "aws-java-sdk-s3", version = "1.11.636")

    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = "5.4.0")
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = "5.4.0")
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.13.2")

    testImplementation(group = "com.bakdata.fluent-kafka-streams-tests", name = "fluent-kafka-streams-tests-junit5", version = "2.0.4")
    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.1.8") {
        exclude(group = "ch.qos.logback")
    }
    testImplementation(group = "log4j", name = "log4j", version = "1.2.17")
    testImplementation(group = "org.slf4j", name = "slf4j-log4j12", version = "1.7.26")
    testImplementation(group = "org.jooq", name = "jool", version = "0.9.14")
}
