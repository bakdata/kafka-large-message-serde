description = "BOM for large message handling in Kafka."

plugins {
    id("java-platform")
}

dependencies {
    constraints {
        api(project(":large-message-core"))
        api(project(":large-message-serde"))
        api(project(":large-message-connect"))
    }
}
