package streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.Properties;

public class CustomerDeduplicationStreamTest {

    protected TopologyTestDriver testDriver;
    protected CustomerDeduplicateJoin streamApp = new CustomerDeduplicateJoin();
    private static final String SCHEMA_REGISTRY_SCOPE = CustomerDeduplicationStreamTest.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    @BeforeEach
    void setUp() {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        var builder = new StreamsBuilder();
        streamApp.createTopology(builder);
        var topology = builder.build();

        testDriver = new TopologyTestDriver(topology, config);

        // TODO do something with the stream
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }
}