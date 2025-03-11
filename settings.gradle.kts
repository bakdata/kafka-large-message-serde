pluginManagement {
    repositories {
        gradlePluginPortal()
        maven(url = "https://s01.oss.sonatype.org/content/repositories/snapshots")
    }
}

rootProject.name = "large-message"

include(
    ":large-message-core",
    ":large-message-serde",
    ":large-message-connect",
    ":large-message-bom",
)
