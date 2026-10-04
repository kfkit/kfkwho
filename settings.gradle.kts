plugins {
    // Downloads a JDK 17 when the host has none (CI runners, cloud sandboxes).
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "kfkwho"
