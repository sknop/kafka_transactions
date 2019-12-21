package streams;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import schema.Customer;

import java.util.Properties;

public class CustomerStream {
    final static String CUSTOMER_TOPIC = "customer";
    final static String BOOTSTRAP_SERVERS = "localhost:9092";
    final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String customerTopic;

    private String bootstrapServers;
    private String schemaRegistryURL;

    private boolean verbose;

    public CustomerStream(Namespace options) {
        customerTopic = options.get("customer_topic");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
        verbose = options.get("verbose");
    }

    private KafkaStreams createStreams(Topology topology) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-stream");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Specify default (de)serializers for record keys and for record values.
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        properties.put(
                StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class.getName());

        return new KafkaStreams(topology, properties);
    }

    private void consume() {
        StreamsBuilder builder = new StreamsBuilder();

//        String rewardsStateStoreName = "rewardsPointsStore";
//
//        KeyValueBytesStoreSupplier storeSupplier =
//                Stores.inMemoryKeyValueStore(rewardsStateStoreName); // 1
//        builder.addStateStore(
//                Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer())); // 2
//
        // KTable<Integer, Customer> existingCustomers = builder.table(customerTopic);
        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic);

        if (verbose)
            existingCustomers.foreach((key, value) -> System.out.println(key + " => " + value));

        KafkaStreams streams = createStreams(builder.build());
        streams.setStateListener((newState, oldState) -> System.out.println("*** Changed state from " +oldState + " to " + newState));
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("CustomerProducer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--customer-topic")
                .type(String.class)
                .setDefault(CUSTOMER_TOPIC)
                .help(String.format("Topic for the customer (default %s)", CUSTOMER_TOPIC));
        parser.addArgument("--bootstrap-servers")
                .type(String.class)
                .setDefault(BOOTSTRAP_SERVERS)
                .help(String.format("Kafka Bootstrap Servers(default %s)", BOOTSTRAP_SERVERS));
        parser.addArgument("--schema-registry")
                .type(String.class)
                .setDefault(SCHEMA_REGISTRY_URL)
                .help(String.format("Schema registry URL(de fault %s)", SCHEMA_REGISTRY_URL));
        parser.addArgument("-v", "--verbose")
                .action(Arguments.storeConst())
                .setDefault(Boolean.FALSE)
                .setConst(Boolean.TRUE)
                .help("If enabled, will print out every message consumed");

        try {
            Namespace options = parser.parseArgs(args);

            CustomerStream cs = new CustomerStream(options);
            cs.consume();

        } catch (ArgumentParserException e) {
            System.err.println(parser.formatHelp());
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}