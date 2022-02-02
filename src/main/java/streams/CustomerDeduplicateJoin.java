package streams;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import picocli.CommandLine;
import schema.Customer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "CustomerDeduplicateJoinCustomerDeduplicateJoin",
        version = "CustomerDeduplicateJoinCustomerDeduplicateJoin 1.0",
        description = "Creates unique version of a customer with updated epoch.")
public class CustomerDeduplicateJoin extends AbstreamStream implements Callable<Integer>  {
    final static String CUSTOMER_TOPIC = "customer";
    final static String CUSTOMER_UNIQUE_TOPIC = "customer-unique-join";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the customer (default = ${DEFAULT-VALUE})")
    private String customerTopic = CUSTOMER_TOPIC;

    @CommandLine.Option(names = {"--unique-topic"},
            description = "Topic for the unique (default = ${DEFAULT-VALUE})")
    private String uniqueTopic = CUSTOMER_UNIQUE_TOPIC;

    public CustomerDeduplicateJoin() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    @Override
    protected String getApplicationName() {
        return "customer-deduplicate-streamcustomer-deduplicate-stream";
    }

    private void consume() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create properties beforehand so that Serdes can be initialised before the topology is built
        createProperties();

        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic);

        Map<String, String> changeLogConfigs = new HashMap<>();
        // put any valid topic configs here
        changeLogConfigs.put("segment.ms", "60000");
        changeLogConfigs.put("segment.bytes", "100000");
        // changeLogConfigs.put("cleanup.policy", "compact,delete");

        Serde<Customer> customerSerde = new SpecificAvroSerde<>();
        Map<String, String> schemaConfig = new HashMap<>();
        properties.forEach((key,value) -> schemaConfig.put(key.toString(),value.toString()));

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
                        if (customer.getCustomerId() == unique.getCustomerId()) {
                            result = unique;
                            result.setEpoch( result.getEpoch() + 1);
                        }
                    }
                    return result;
                })
                .peek((k,v) -> System.out.println("Peeked key = " + k + " value = " + v))
                .filter( ((key, value) -> (value != null)))
                .to(uniqueTopic);

        KafkaStreams streams = createStreams(builder.build(), false);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    @Override
    public Integer call() throws Exception {
        consume();

        return 0;
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new CustomerDeduplicateJoin()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}