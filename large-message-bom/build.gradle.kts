description = "BOM for large message handling in Kafka."

plugins {
    id("java-platform")
}

dependencies {
    constraints {
        api(project(":large-message-core"))
        api(project(":large-message-serde"))
        api(project(":large-message-connect"))
        api(project(":large-message-amazon-s3"))
        api(project(":large-message-google-cloud-storage"))
        api(project(":large-message-azure-blob-storage"))
    }
}
