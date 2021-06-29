package admin;

import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import static org.junit.jupiter.api.Assertions.*;

public class AdminTest {
    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.0"));

    @Test
    public void testHelloWorld() {
        kafka.start();
        assertEquals(1,1);
        assertNotNull(kafka.getBootstrapServers());
        kafka.stop();
    }
}
