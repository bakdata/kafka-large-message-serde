description = "Kafka Connect converter that stores large messages on a blob storage, such as Amazon S3 and Azure Blob Storage"



dependencies {
    api(project(":large-message-core"))
    val kafkaVersion: String by project
    api(group = "org.apache.kafka", name = "connect-api", version = kafkaVersion)
    compileOnly(group = "org.apache.kafka", name = "connect-runtime", version = kafkaVersion)

    testImplementation(project(":large-message-serde"))
    val junitVersion: String by project
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junitVersion)
    testImplementation(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junitVersion)
    testRuntimeOnly(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junitVersion)
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.13.2")

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.1.8") {
        exclude(group = "ch.qos.logback")
    }
    testImplementation(group = "log4j", name = "log4j", version = "1.2.17")
    testImplementation(group = "org.slf4j", name = "slf4j-log4j12", version = "1.7.26")
    testImplementation(group = "org.jooq", name = "jool-java-8", version = "0.9.14")
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaVersion)
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
}
