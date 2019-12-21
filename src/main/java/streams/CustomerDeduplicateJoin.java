package streams;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import schema.Customer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CustomerDeduplicateJoin {
    final static String CUSTOMER_TOPIC = "customer";
    final static String CUSTOMER_UNIQUE_TOPIC = "customer-unique-join";
    final static String BOOTSTRAP_SERVERS = "localhost:9092";
    final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String customerTopic;
    private String uniqueTopic;

    private String bootstrapServers;
    private String schemaRegistryURL;

    public CustomerDeduplicateJoin(Namespace options) {
        customerTopic = options.get("customer_topic");
        uniqueTopic = options.get("unique_topic");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    private KafkaStreams createStreams(Topology topology) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "customer-deduplicate-stream");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Specify default (de)serializers for record keys and for record values.
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        return new KafkaStreams(topology, properties);
    }

    private void consume() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic);

        Map<String, String> changeLogConfigs = new HashMap<>();
        // put any valid topic configs here
        changeLogConfigs.put("segment.ms", "60000");
        changeLogConfigs.put("segment.bytes", "100000");
        changeLogConfigs.put("cleanup.policy", "compact,delete");

        Serde<Customer> customerSerde = new SpecificAvroSerde<>();
        Map<String, String> schemaConfig =
                Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        customerSerde.configure(schemaConfig, false);

        KTable<Integer, Customer> uniqueCustomers = builder.table(uniqueTopic, Consumed.with(Serdes.Integer(), customerSerde),
                Materialized.<Integer, Customer, KeyValueStore<Bytes, byte[]>>as("uniqueCustomerTableStore")
                        .withLoggingEnabled(changeLogConfigs).
                        withCachingEnabled()
        );

        existingCustomers
                .leftJoin(uniqueCustomers, (customer, unique) -> {
                    Customer result = customer;
                    if (unique != null) {
                        if (customer.getCustomerId().equals(unique.getCustomerId())) {
                            result = unique;
                            result.setEpoch( result.getEpoch() + 1);
                        }
                    }
                    return result;
                })
                .peek((k,v) -> System.out.println("Peeked key = " + k + " value = " + v))
                .filter( ((key, value) -> (value != null)))
                .to(uniqueTopic);

        KafkaStreams streams = createStreams(builder.build());
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("CustomerDeduplicateJoin").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--customer-topic")
                .type(String.class)
                .setDefault(CUSTOMER_TOPIC)
                .help(String.format("Topic for the customer (default %s)", CUSTOMER_TOPIC));
        parser.addArgument("--unique-topic")
                .type(String.class)
                .setDefault(CUSTOMER_UNIQUE_TOPIC)
                .help(String.format("Unique topic for the customer (default %s)", CUSTOMER_UNIQUE_TOPIC));
        parser.addArgument("--bootstrap-servers")
                .type(String.class)
                .setDefault(BOOTSTRAP_SERVERS)
                .help(String.format("Kafka Bootstrap Servers(default %s)", BOOTSTRAP_SERVERS));
        parser.addArgument("--schema-registry")
                .type(String.class)
                .setDefault(SCHEMA_REGISTRY_URL)
                .help(String.format("Schema registry URL(de fault %s)", SCHEMA_REGISTRY_URL));

        try {
            Namespace options = parser.parseArgs(args);

            CustomerDeduplicateJoin app = new CustomerDeduplicateJoin(options);

            app.consume();
        } catch (ArgumentParserException e) {
            System.err.println(parser.formatHelp());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}