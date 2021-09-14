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
    testImplementation(group = "org.assertj", name = "assertj-core", version = "3.20.2")

    testImplementation(group = "com.adobe.testing", name = "s3mock-junit5", version = "2.1.8") {
        exclude(group = "ch.qos.logback")
        exclude(group = "org.apache.logging.log4j", module = "log4j-to-slf4j")
    }
    val log4jVersion = "2.14.1"
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-core", version = log4jVersion)
    testImplementation(group = "org.apache.logging.log4j", name = "log4j-slf4j-impl", version = log4jVersion)
    testImplementation(group = "org.jooq", name = "jool-java-8", version = "0.9.14")
    testImplementation(group = "net.mguenther.kafka", name = "kafka-junit", version = kafkaVersion) {
        exclude(group = "org.slf4j", module = "slf4j-log4j12")
    }
    testImplementation(group = "org.apache.kafka", name = "connect-file", version = kafkaVersion)
}
