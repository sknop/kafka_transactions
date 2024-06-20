package streams;

import common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import picocli.CommandLine;
import schema.Customer;

import java.util.Properties;

@CommandLine.Command(name = "CustomerDeduplicateJoinCustomerDeduplicateJoin",
        version = "CustomerDeduplicateJoinCustomerDeduplicateJoin 1.0",
        description = "Creates unique version of a customer with updated epoch.")
public class CustomerDeduplicateJoin extends AbstractStream {
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
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        if (stateDir != null) {
            properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        }
    }

    @Override
    protected String getApplicationName() {
        return "customer-deduplicate-streamcustomer-deduplicate-stream";
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic, Consumed.with(Serdes.Integer(), SerdeGenerator.getSerde(properties)));

//        Map<String, String> changeLogConfigs = new HashMap<>();
//        // put any valid topic configs here
//        changeLogConfigs.put("segment.ms", "60000");
//        changeLogConfigs.put("segment.bytes", "100000");
//        // changeLogConfigs.put("cleanup.policy", "compact,delete");

        KTable<Integer, Customer> uniqueCustomers = builder.table(uniqueTopic, Consumed.with(Serdes.Integer(), SerdeGenerator.getSerde(properties)),
                Materialized.<Integer, Customer, KeyValueStore<Bytes, byte[]>>as("uniqueCustomerTableStore")
                        // .withLoggingEnabled(changeLogConfigs)
                        .withCachingEnabled()
        );

        createTopologyWithStreams(existingCustomers, uniqueCustomers);
    }

    /* potentially useful for test cases to separate this step out? */
    protected void createTopologyWithStreams(KStream<Integer, Customer> existingCustomers, KTable<Integer, Customer> uniqueCustomers) {
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
                .to(uniqueTopic, Produced.with(Serdes.Integer(), SerdeGenerator.getSerde(properties)));
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