plugins {
    java
    `jvm-test-suite`
    `maven-publish`
    id("me.champeau.jmh") version "0.7.4"
}

group = "dev.kfkit"
// A release passes its version, the tag without the v: -Pversion=0.1.0. It
// must be X.Y.Z, so no SNAPSHOT reaches a release jar; without it the build
// is the development version.
val developmentVersion = "0.2.0-SNAPSHOT"
version = providers.gradleProperty("version").orNull?.let { requested ->
    require(Regex("""\d+\.\d+\.\d+""").matches(requested)) {
        "-Pversion=$requested: a release version is X.Y.Z, without a v or a suffix"
    }
    requested
} ?: developmentVersion

// The broker provides the Kafka classes at runtime; the jar ships without
// dependencies. Compile against the newest supported release; the test matrix
// in CI runs the jar on every supported broker.
val kafkaVersion = "4.4.0"

// The broker image the integration test runs the jar on: -PbrokerVersion=4.1.2.
val brokerVersion = providers.gradleProperty("brokerVersion").getOrElse(kafkaVersion)

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
    // What Maven Central asks for next to the jar.
    withSourcesJar()
    withJavadocJar()
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
    // TagValuesTest logs through the API to prove it captures what the broker libraries log.
    testCompileOnly("org.slf4j:slf4j-api:2.0.20")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")

    jmh("org.apache.kafka:kafka-metadata:$kafkaVersion")
    jmh("org.apache.kafka:kafka-clients:$kafkaVersion")
    jmh("org.apache.kafka:kafka-server-common:$kafkaVersion")
    jmhRuntimeOnly("org.slf4j:slf4j-nop:2.0.20")
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
    // RecorderConfigTest and MetricsDocTest read these; a change to them reruns the tests.
    inputs.dir("docs")
    inputs.file("jmx-exporter/kfkwho.yml")
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

// A wrong doc comment fails the build, as a compiler warning does; a missing
// one does not.
tasks.javadoc {
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        addBooleanOption("Xdoclint:all,-missing", true)
        addBooleanOption("Werror", true)
    }
}

// ./gradlew publishToMavenLocal -Pversion=X.Y.Z: the jar, the sources, the
// javadoc and a POM with what Maven Central requires. Nothing here publishes
// to Central; the maintainer uploads, see docs/releasing.md. The POM names no
// dependency, as the jar has none: the Kafka classes are compileOnly, the
// broker's, and what the tests and the benchmarks use is not part of what is
// published.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name = "kfkwho"
                description = "Who accesses what on an Apache Kafka cluster: the broker's " +
                    "StandardAuthorizer plus JMX metrics per principal, client id, resource and operation."
                url = "https://github.com/kfkit/kfkwho"
                licenses {
                    license {
                        name = "Apache-2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                        distribution = "repo"
                    }
                }
                developers {
                    developer {
                        id = "iabramov"
                        name = "Ivan Abramov"
                        url = "https://github.com/iabramov"
                    }
                }
                scm {
                    url = "https://github.com/kfkit/kfkwho"
                    connection = "scm:git:https://github.com/kfkit/kfkwho.git"
                    developerConnection = "scm:git:ssh://git@github.com/kfkit/kfkwho.git"
                }
                issueManagement {
                    system = "GitHub"
                    url = "https://github.com/kfkit/kfkwho/issues"
                }
            }
        }
    }
}

// Maven Central gets the jar of the GitHub release, the one the release
// workflow built and signed, with this POM, the sources and the javadoc next
// to it. Gradle's module metadata would name the size and hashes of a jar
// built here instead, so it is not published; the POM is all there is.
tasks.withType<GenerateModuleMetadata>().configureEach {
    enabled = false
}

// The hot path, measured: ./gradlew jmh. Not part of build; check only
// compiles the benchmarks so they cannot rot. Iterations, forks and threads
// are on the benchmark classes; results and the allocation profile land in
// build/results/jmh/results.json.
jmh {
    jmhVersion = "1.37"
    resultFormat = "JSON"
    resultsFile = layout.buildDirectory.file("results/jmh/results.json")
    profilers = listOf("gc")
}

tasks.check {
    dependsOn(tasks.named("jmhClasses"))
}
