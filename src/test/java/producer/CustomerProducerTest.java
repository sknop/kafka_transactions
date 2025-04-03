package producer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import testcontainers.SchemaRegistryContainer;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.fail;

public class CustomerProducerTest {
    public static Network network = Network.newNetwork();

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.9.0"))
            .withNetwork(network);

    @Container
    public static SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:7.9.0"))
            .withKafka(kafka);

    @BeforeAll
    public static void startUp() {
        kafka.start();
        schemaRegistry.start();
    }

    @AfterAll
    public static void shutdown() {
        schemaRegistry.stop();
        kafka.stop();
    }

    @Test
    public void testProducer() {
        var producer = new CustomerProducer(kafka.getBootstrapServers(), schemaRegistry.getSchemaRegistries(), 1, "customer");
        try {
            producer.produce();
        } catch (IOException e) {
            fail("Should not throw an exception");
        }
    }
}
