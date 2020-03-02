description = "Base module for Kafka plugins that store large messages on Amazon S3"



dependencies {
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "kafka-clients", version = kafkaVersion)

    val confluentVersion: String by project
    api(group = "io.confluent", name = "common-config", version = confluentVersion)

    implementation(group = "org.slf4j", name = "slf4j-api", version = "1.7.26")
    api(group = "com.amazonaws", name = "aws-java-sdk-s3", version = "1.11.636")
    implementation(group = "com.google.guava", name = "guava", version = "28.2-jre")

    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = "5.4.0")
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = "5.4.0")
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.13.2")
}
