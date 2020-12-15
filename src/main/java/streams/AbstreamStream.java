package streams;

import common.AbstractBase;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import picocli.CommandLine;

import java.util.Properties;

@CommandLine.Command(
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false)
public abstract class AbstreamStream extends AbstractBase {
    @CommandLine.Option(names = {"--bootstrap-servers"},
            description = "Bootstrap Servers (default = ${DEFAULT-VALUE})")
    protected String bootstrapServers;

    @CommandLine.Option(names = {"--schema-registry"},
            description = "Schema Registry (default = ${DEFAULT-VALUE})")
    protected String schemaRegistryURL;

    @CommandLine.Option(names = {"-c", "--config-file"},
            description = "If provided, content will be added to the properties")
    protected String configFile = null;

    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    protected boolean verbose = false;

    @CommandLine.Option(names = {"--enable-monitoring-interceptor"},
            description = "Enable MonitoringInterceptors (for Control Center)")
    protected boolean monitoringInterceptors = false;

    protected KafkaStreams createStreams(Topology topology) {
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY);

        AbstractBase.readConfigFile(properties, configFile);

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationName());
        if (bootstrapServers != null) {
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        if (schemaRegistryURL != null) {
            properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        }

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Specify default (de)serializers for record keys and for record values.
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        // properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        if (monitoringInterceptors) {
            properties.put(
                    StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        }

        //        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        //                LogAndContinueExceptionHandler.class.getName());

        addConsumerProperties(properties);

        return new KafkaStreams(topology, properties);
    }

    protected abstract void addConsumerProperties(Properties properties);

    protected abstract String getApplicationName();

}
