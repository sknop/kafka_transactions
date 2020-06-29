package streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import picocli.CommandLine;
import schema.Customer;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "CustomerStream",
        version = "CustomerStream 1.0",
        description = "Reads Customer objects in Avro format from a stream.")
public class CustomerStream extends AbstreamStream implements Callable<Integer> {
    final static String CUSTOMER_TOPIC = "customer";
    final static String BOOTSTRAP_SERVERS = "localhost:9092";
    final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String customerTopic = CUSTOMER_TOPIC;

    public CustomerStream() {
    }

    @Override
    protected String getApplicationName() {
        return "customer-stream";
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


    @Override
    public Integer call() {
        consume();

        return 0;
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new CustomerStream()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}