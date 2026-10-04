plugins {
    java
    `jvm-test-suite`
}

group = "dev.kfkit"
version = "0.1.0-SNAPSHOT"

// The broker provides the Kafka classes at runtime; the jar ships without
// dependencies. Compile against the newest supported release; the test matrix
// in CI runs the jar on every supported broker.
val kafkaVersion = "4.3.1"

// The broker image the integration test runs the jar on: -PbrokerVersion=4.1.2.
val brokerVersion = providers.gradleProperty("brokerVersion").getOrElse(kafkaVersion)

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
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")
}

// The JMX Exporter agent the broker in the integration test runs with.
val jmxExporterAgent = configurations.create("jmxExporterAgent") {
    isTransitive = false
}

dependencies {
    jmxExporterAgent("io.prometheus.jmx:jmx_prometheus_javaagent:1.0.1")
}

testing {
    suites {
        // A real broker in a container with the built jar as its authorizer.
        // Not part of build or check unless -PintegrationTest is set; CI runs
        // it once per supported broker release.
        register<JvmTestSuite>("integrationTest") {
            useJUnitJupiter("6.1.3")
            dependencies {
                implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
                implementation("org.testcontainers:testcontainers-kafka:2.0.5")
                runtimeOnly("org.slf4j:slf4j-simple:2.0.20")
            }
            targets.all {
                testTask.configure {
                    dependsOn(tasks.jar)
                    inputs.files(tasks.jar, jmxExporterAgent)
                    inputs.file("jmx-exporter/kfkwho.yml")
                    inputs.property("brokerVersion", brokerVersion)
                    systemProperty("kfkwho.it.jar", tasks.jar.get().archiveFile.get().asFile.absolutePath)
                    systemProperty("kfkwho.it.agent", jmxExporterAgent.singleFile.absolutePath)
                    systemProperty("kfkwho.it.rules", file("jmx-exporter/kfkwho.yml").absolutePath)
                    systemProperty("kfkwho.it.image", "apache/kafka:$brokerVersion")
                    testLogging {
                        events("passed", "skipped", "failed")
                        showStandardStreams = false
                    }
                }
            }
        }
    }
}

if (providers.gradleProperty("integrationTest").isPresent) {
    tasks.check {
        dependsOn(testing.suites.named("integrationTest"))
    }
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
