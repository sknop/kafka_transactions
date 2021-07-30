package admin;

import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AdminTest {
    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"));

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
            assertEquals(nodes.size(), 1);

            List<String> topics = admin.getTopics();
            assertEquals(topics.size(), 0);
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
            assertEquals(topics.size(), 0);
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

            assertEquals(result.replicationFactor(topicName).get(),(short)1);
            assertEquals(result.numPartitions(topicName).get(),4);

            List<String> topics = admin.getTopics();
            assertEquals(topics.size(), 1);
        } catch (ExecutionException | InterruptedException e) {
            fail("Should not throw an exception here");
        }
    }

}
