pluginManagement {
    repositories {
        gradlePluginPortal()
        maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    }
}

rootProject.name = "large-message"

include(
    ":large-message-core",
    ":large-message-serde",
    ":large-message-connect",
    ":large-message-bom",
)
