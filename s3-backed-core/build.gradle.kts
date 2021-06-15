description = "Base module for Kafka plugins that store large messages on Amazon S3"



dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)

    val confluentVersion: String by project
    api(group = "io.confluent", name = "common-config", version = confluentVersion)

    implementation(group = "org.slf4j", name = "slf4j-api", version = "1.7.26")
    api(group = "com.amazonaws", name = "aws-java-sdk-s3", version = "1.11.636")
    api(group = "com.amazonaws", name = "aws-java-sdk-sts", version = "1.11.636")
    api(group = "com.azure", name = "azure-storage-blob", version = "12.12.0")
    implementation(group = "com.google.guava", name = "guava", version = "28.2-jre")

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.13.2")
    val mockitoVersion = "2.28.2"
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.1.8") {
        exclude(group = "ch.qos.logback")
    }
    testImplementation(group = "log4j", name = "log4j", version = "1.2.17")
    testImplementation(group = "org.slf4j", name = "slf4j-log4j12", version = "1.7.26")
}
