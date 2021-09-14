description = "Base module for Kafka plugins that store large messages on a blob storage, such as Amazon S3 and Azure Blob Storage"



dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)

    val confluentVersion: String by project
    api(group = "io.confluent", name = "common-config", version = confluentVersion)

    implementation(group = "org.slf4j", name = "slf4j-api", version = "1.7.32")
    val awsVersion = "1.12.66"
    api(group = "com.amazonaws", name = "aws-java-sdk-s3", version = awsVersion)
    api(group = "com.amazonaws", name = "aws-java-sdk-sts", version = awsVersion)
    api(group = "com.azure", name = "azure-storage-blob", version = "12.13.0")
    implementation(group = "com.google.guava", name = "guava", version = "30.1.1-jre")

    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")
    val mockitoVersion = "3.12.4"
    testImplementation(group = "org.mockito", name = "mockito-core", version = mockitoVersion)
    testImplementation(group = "org.mockito", name = "mockito-junit-jupiter", version = mockitoVersion)

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.1.8") {
        exclude(group = "ch.qos.logback")
        exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
    }
    val log4jVersion = "2.14.1"
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    testImplementation(group = "org.testcontainers", name = "junit-jupiter", version = "1.16.0")
}
