package processor;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import schema.Customer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CustomerDeduplicator {
    final static String CUSTOMER_TOPIC = "customer";
    final static String UNIQUE_TOPIC = "customer-unique";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String customerTopic;
    private String uniqueTopic;

    private String bootstrapServers;
    private String schemaRegistryURL;

    public CustomerDeduplicator(Namespace options) {
        customerTopic = options.get("customer_topic");
        uniqueTopic = options.get("unique_topic");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    /**
     * Discards duplicate records from the input stream.
     *
     * Duplicate records are detected based on an event ID;  in this simplified example, the record
     * value is the event ID.  The transformer remembers known event IDs in an associated window state
     * store, which automatically purges/expires event IDs from the store after a certain amount of
     * time has passed to prevent the store from growing indefinitely.
     *
     * Note: This code is for demonstration purposes and was not tested for production usage.
     */
    private static class DeduplicationTransformer<K, V, R> implements Transformer<K, V, KeyValue<K, V>> {

        private ProcessorContext context;

        /**
         * Key: Customer ID
         * Value: Customer object (AVRO)
         */
        private KeyValueStore<K, V> customerStore;

        /**
         *
         */
        DeduplicationTransformer() {
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(final ProcessorContext context) {
            this.context = context;
            customerStore = (KeyValueStore<K, V>) context.getStateStore("unique-customer-store");
        }

        @SuppressWarnings("unchecked")
        @Override
        public KeyValue<K, V> transform(final K key, final V value) {
            KeyValue<K, V> output;

            V storedValue = customerStore.get(key);
            if (storedValue != null) {
                // duplicate, increase epoch
                Customer existingCustomer = (Customer) storedValue;
                Customer updatedCustomer = (Customer) value;

                Customer newCustomer = new Customer(
                        existingCustomer.getCustomerId(),
                        existingCustomer.getFirstName(),
                        existingCustomer.getLastName(),
                        existingCustomer.getEmail(),
                        updatedCustomer.getUpdate(),
                        existingCustomer.getEpoch() + 1
                );
                output = KeyValue.pair(key, (V) newCustomer);
            }
            else {
                output = KeyValue.pair(key, value);
            }

            customerStore.put(output.key, output.value);

            return output;
        }

        @Override
        public void close() {
            // Note: The store should NOT be closed manually here via `customerStore.close()`!
            // The Kafka Streams API will automatically close stores when necessary.
        }

    }

    private void consume() {
        StreamsBuilder builder = new StreamsBuilder();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-customer-stream");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Use a temporary directory for storing state, which will be automatically removed after the test.
        // streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());

        Serde<Customer> customerSerde = new SpecificAvroSerde<>();
        Map<String, String> schemaConfig =
                Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        customerSerde.configure(schemaConfig, false);

        KeyValueBytesStoreSupplier deduplicationStoreSupplier = Stores.persistentKeyValueStore ("unique-customer-store");
        StoreBuilder<KeyValueStore<Integer, Customer>> customerStoreBuilder =
                Stores.keyValueStoreBuilder(deduplicationStoreSupplier, Serdes.Integer(), customerSerde);

        builder.addStateStore(customerStoreBuilder);

        KStream<Integer, Customer> input = builder.stream(customerTopic);
        KStream<Integer, Customer> deduplicated = input
                .transform(() -> new DeduplicationTransformer<>(),"unique-customer-store")
                .peek((k,v) -> System.out.println("Result = " + k + " value = " + v)
        );
        deduplicated.to(uniqueTopic);

        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.start();

        System.out.println("Deduplicator is up");

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("CustomerDeduplicator").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--customer-topic")
                .type(String.class)
                .setDefault(CUSTOMER_TOPIC)
                .help(String.format("Topic for the customer (default %s)", CUSTOMER_TOPIC));
        parser.addArgument("--unique-topic")
                .type(String.class)
                .setDefault(UNIQUE_TOPIC)
                .help(String.format("(Output) Topic for unique customers (default %s)", UNIQUE_TOPIC));
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

            CustomerDeduplicator cs = new CustomerDeduplicator(options);
            cs.consume();

        } catch (ArgumentParserException e) {
            System.err.println(parser.formatHelp());
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}
