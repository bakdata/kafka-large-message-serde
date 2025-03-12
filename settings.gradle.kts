pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

rootProject.name = "large-message"

include(
    ":large-message-core",
    ":large-message-serde",
    ":large-message-connect",
    ":large-message-bom",
)
