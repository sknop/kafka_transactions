package streams;

import common.AbstractBase;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import picocli.CommandLine;

import java.util.Properties;

public abstract class AbstreamStream extends AbstractBase {
    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    protected boolean verbose = false;

    @CommandLine.Option(names = {"--scale"},
            description = "If greater than 1, Stream app will increase threads to the number provided")
    protected int scale = 1;

    @CommandLine.Option(names = {"--state-dir"},
            description = "If set, use as the state.dir parameter for this stream")
    protected String stateDir;

    public AbstreamStream() {
    }

    @Override
    protected void createProperties() {
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationName());

        readConfigFile(properties);

        if (bootstrapServers != null) {
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        if (schemaRegistryURL != null) {
            properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        }

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        if (stateDir != null) {
            properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        }

        // Specify default (de)serializers for record keys and for record values.
        // properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        // properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        // properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        addConsumerProperties(properties);
    }

    protected KafkaStreams createStreams(Topology topology) {
        return this.createStreams(topology, true);
    }

    protected KafkaStreams createStreams(Topology topology, boolean createProperties) {
        if (createProperties)
            createProperties();

        return new KafkaStreams(topology, properties);
    }

    protected abstract void addConsumerProperties(Properties properties);

    protected abstract String getApplicationName();

}
