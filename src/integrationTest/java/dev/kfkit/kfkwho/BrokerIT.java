/*
 * Copyright 2026 Ivan Abramov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.kfkit.kfkwho;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * The jar that ships, on a real broker: {@code apache/kafka} with the built
 * jar on its classpath as the authorizer and the JMX Exporter agent reading
 * the rules in {@code jmx-exporter/}. A producer and a consumer touch
 * {@code orders}; the exporter's {@code /metrics} must show both.
 *
 * <p>Gradle passes the image ({@code -PbrokerVersion}), the jar, the agent
 * and the rules as system properties.
 */
class BrokerIT {

    private static final String TOPIC = "orders";
    private static final String PRODUCER = "it-producer";
    private static final String CONSUMER = "it-consumer";
    private static final int EXPORTER_PORT = 9404;
    private static final String DIR = "/opt/kfkwho/";

    private static final String NO_DOCKER =
            "Docker is not available; the broker integration test needs it and is skipped";

    private static KafkaContainer broker;

    @BeforeAll
    static void start() {
        if (!DockerClientFactory.instance().isDockerAvailable()) {
            // Assumed again in each test, where the reason reaches the report.
            return;
        }
        broker = new KafkaContainer(DockerImageName.parse(property("kfkwho.it.image")))
                .withCopyFileToContainer(file("kfkwho.it.jar"), DIR + "kfkwho.jar")
                .withCopyFileToContainer(file("kfkwho.it.agent"), DIR + "jmx_prometheus_javaagent.jar")
                .withCopyFileToContainer(file("kfkwho.it.rules"), DIR + "kfkwho.yml")
                .withEnv("CLASSPATH", DIR + "kfkwho.jar")
                .withEnv("KAFKA_OPTS",
                        "-javaagent:" + DIR + "jmx_prometheus_javaagent.jar=" + EXPORTER_PORT + ":" + DIR + "kfkwho.yml")
                // By name: the broker loads the class from the jar, not from this test's classpath.
                .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "dev.kfkit.kfkwho.MeteredStandardAuthorizer")
                // Plaintext clients, the broker itself and the controller are all ANONYMOUS.
                .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS")
                .withExposedPorts(9092, EXPORTER_PORT);
        broker.start();
    }

    @AfterAll
    static void stop() {
        if (broker != null) {
            broker.stop();
        }
    }

    @Test
    void exporterShowsWhoProducedAndWhoConsumed() throws Exception {
        // In CI a skip would pass the matrix without running anything.
        assertFalse(broker == null && "true".equals(System.getenv("GITHUB_ACTIONS")), NO_DOCKER);
        assumeTrue(broker != null, NO_DOCKER);
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
        produce();
        consume();

        awaitSamples(Duration.ofSeconds(30), List.of(
                Map.of("client_id", PRODUCER, "resource", TOPIC, "operation", "WRITE", "api", "PRODUCE",
                        "result", "ALLOWED"),
                Map.of("client_id", CONSUMER, "resource", TOPIC, "operation", "READ", "api", "FETCH")));

        String log = broker.getLogs();
        List<String> errors = log.lines()
                .filter(line -> line.contains("NullPointerException")
                        || line.contains("ERROR") && line.toLowerCase(Locale.ROOT).contains("kfkwho"))
                .toList();
        assertEquals(List.of(), errors, "errors in the broker log");
    }

    private static void produce() throws Exception {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER);
        try (KafkaProducer<String, String> producer =
                new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            for (int i = 0; i < 10; i++) {
                producer.send(new ProducerRecord<>(TOPIC, "order-" + i, "{}")).get();
            }
        }
    }

    private static void consume() {
        Properties config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "it-group");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(TOPIC));
            int received = 0;
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (received < 10 && System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                received += records.count();
            }
            assertEquals(10, received, "records consumed from " + TOPIC);
        }
    }

    /**
     * Scrapes {@code /metrics} until a {@code kfkwho_access_requests_total}
     * sample carries each set of labels; fails with the samples it saw.
     */
    private static void awaitSamples(Duration timeout, List<Map<String, String>> wanted) throws Exception {
        URI uri = URI.create("http://" + broker.getHost() + ":" + broker.getMappedPort(EXPORTER_PORT) + "/metrics");
        HttpClient http = HttpClient.newBuilder().proxy(HttpClient.Builder.NO_PROXY).build();
        long deadline = System.nanoTime() + timeout.toNanos();
        String body = "";
        while (true) {
            try {
                body = http.send(HttpRequest.newBuilder(uri).build(), HttpResponse.BodyHandlers.ofString()).body();
            } catch (IOException e) {
                body = e.toString();
            }
            List<String> samples = body.lines().filter(line -> line.startsWith("kfkwho_access_requests_total{"))
                    .toList();
            boolean all = true;
            for (Map<String, String> labels : wanted) {
                all &= samples.stream().anyMatch(sample -> hasLabels(sample, labels));
            }
            if (all) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no kfkwho_access_requests_total sample with " + wanted
                        + "; samples:\n" + samples.stream().collect(Collectors.joining("\n")));
            }
            Thread.sleep(500);
        }
    }

    private static boolean hasLabels(String sample, Map<String, String> labels) {
        return labels.entrySet().stream()
                .allMatch(label -> sample.contains(label.getKey() + "=\"" + label.getValue() + "\""));
    }

    private static String bootstrap() {
        return broker.getBootstrapServers();
    }

    private static MountableFile file(String property) {
        return MountableFile.forHostPath(Path.of(property(property)));
    }

    private static String property(String name) {
        String value = System.getProperty(name);
        if (value == null) {
            throw new IllegalStateException(name + " is not set; run the test through ./gradlew integrationTest");
        }
        return value;
    }
}
