plugins {
    java
}

group = "dev.kfkit"
version = "0.1.0-SNAPSHOT"

// The broker provides the Kafka classes at runtime; the jar ships without
// dependencies. Compile against the newest supported release; the test matrix
// in CI runs the jar on every supported broker.
val kafkaVersion = "4.3.1"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.kafka:kafka-metadata:$kafkaVersion")
    compileOnly("org.apache.kafka:kafka-clients:$kafkaVersion")
    compileOnly("org.apache.kafka:kafka-server-common:$kafkaVersion")

    testImplementation("org.apache.kafka:kafka-metadata:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    testImplementation("org.apache.kafka:kafka-server-common:$kafkaVersion")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
    options.encoding = "UTF-8"
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
            "Implementation-Vendor" to "kfkit",
        )
    }
}
