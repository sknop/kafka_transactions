package testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    public static final int SCHEMA_REGISTRY_PORT = 8081;

    private static final int PORT_NOT_ASSIGNED = -1;

    protected ConfluentKafkaContainer kafkaContainer;
    private int port = PORT_NOT_ASSIGNED;

    public SchemaRegistryContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);

        withExposedPorts(SCHEMA_REGISTRY_PORT);

        withEnv("host.name", "schema-registry");
        withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
        withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT);
    }

    public SchemaRegistryContainer withKafka(ConfluentKafkaContainer kafkaContainer) {
        this.kafkaContainer = kafkaContainer;

        dependsOn(kafkaContainer);
        withNetwork(kafkaContainer.getNetwork());
        withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaContainer.getNetworkAliases().getFirst() + ":9093");

        return this;
    }

    public String getSchemaRegistries() {
        if (port == PORT_NOT_ASSIGNED) {
            throw new IllegalStateException("You should start Schema Registry container first");
        }
        return String.format("http://%s:%s",getHost(), port);
    }

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        port = getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}
