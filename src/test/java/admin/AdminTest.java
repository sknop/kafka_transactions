package admin;

import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.*;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AdminTest {
    @Container
    public static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.0.0"));

    @BeforeAll
    public static void startUp() {
        kafka.start();
    }

    @AfterAll
    public static void shutdown() {
        kafka.stop();
    }

    @Test
    @Order(1)
    public void testBootstrapServers() {
        assertNotNull(kafka.getBootstrapServers());
    }

    @Test
    @Order(2)
    public void testAdminClientCreation() {
        Admin admin = new Admin(kafka.getBootstrapServers());
        try {
            Collection<Node> nodes = admin.getKafkaNodes();
            assertEquals(1, nodes.size());

            List<String> topics = admin.getTopics();
            assertEquals(0, topics.size());
        } catch (ExecutionException | InterruptedException e) {
            fail("Should not throw an exception here");
        }
    }

    @Test
    @Order(3)
    public void testNoTopics() {
        Admin admin = new Admin(kafka.getBootstrapServers());
        try {
            List<String> topics = admin.getTopics();
            assertEquals(0, topics.size());
        } catch (ExecutionException | InterruptedException e) {
            fail("Should not throw an exception here");
        }
    }

    @Test
    @Order(4)
    public void testCreateTopic() {
        Admin admin = new Admin(kafka.getBootstrapServers());
        String topicName = "test-topic";

        try {
            CreateTopicsResult result = admin.createTopic(topicName, 4, (short) 1);

            assertEquals((short)1, result.replicationFactor(topicName).get());
            assertEquals(4, result.numPartitions(topicName).get());

            List<String> topics = admin.getTopics();
            assertEquals(1, topics.size());
        } catch (ExecutionException | InterruptedException e) {
            fail("Should not throw an exception here");
        }
    }

}
