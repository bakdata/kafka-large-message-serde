pluginManagement {
    repositories {
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
    repositories {
        mavenCentral()
        maven(url = "https://central.sonatype.com/repository/maven-snapshots")
    }
}

rootProject.name = "large-message"

include(
    ":large-message-core",
    ":large-message-serde",
    ":large-message-connect",
    ":large-message-amazon-s3",
    ":large-message-google-cloud-storage",
    ":large-message-azure-blob-storage",
    ":large-message-bom",
)
