package producer;

import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import testcontainers.SchemaRegistryContainer;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.fail;

public class CustomerProducerTest {
    @RegisterExtension
    @Order(1)
    public static Network network = Network.newNetwork();

    @RegisterExtension
    @Order(2)
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"))
            .withNetwork(network);

    @RegisterExtension
    @Order(3)
    public static SchemaRegistryContainer schemaRegistry = new SchemaRegistryContainer(DockerImageName.parse("confluentinc/cp-schema-registry:6.2.0"))
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
