buildscript {
    repositories {
        maven {
            url = uri("https://s01.oss.sonatype.org/content/repositories/snapshots")
        }
    }
    dependencies {
        classpath("com.bakdata.gradle:sonar:1.5.3-SNAPSHOT")
        classpath("com.bakdata.gradle:release:1.5.3-SNAPSHOT")
        classpath("com.bakdata.gradle:sonatype:1.5.3-SNAPSHOT")
    }
}

plugins {
    id("io.freefair.lombok") version "8.4"
}
apply(plugin = "com.bakdata.release")
apply(plugin = "com.bakdata.sonar")
apply(plugin = "com.bakdata.sonatype")

allprojects {
    group = "com.bakdata.kafka"

    tasks.withType<Test> {
        maxParallelForks = 4
        useJUnitPlatform()
    }

    repositories {
        mavenCentral()
    }
}

configure<com.bakdata.gradle.SonatypeSettings> {
    developers {
        developer {
            name.set("Sven Lehmann")
            id.set("SvenLehmann")
        }
        developer {
            name.set("Torben Meyer")
            id.set("torbsto")
        }
        developer {
            name.set("Philipp Schirmer")
            id.set("philipp94831")
        }
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "java-test-fixtures")
    apply(plugin = "io.freefair.lombok")

    configure<JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(11)
        }
    }
}
