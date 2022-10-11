package streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import picocli.CommandLine;
import schema.Customer;

import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "CustomerStream",
        version = "CustomerStream 1.0",
        description = "Reads Customer objects in Avro format from a stream.")
public class CustomerStream extends AbstreamStream implements Callable<Integer> {
    final static String CUSTOMER_TOPIC = "customer";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String customerTopic = CUSTOMER_TOPIC;

    public CustomerStream() {
    }

    @Override
    protected String getApplicationName() {
        return "customer-stream";
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    private void consume() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic);

        if (verbose)
            existingCustomers.foreach((key, value) -> System.out.println(key + " => " + value));

        KafkaStreams streams = createStreams(builder.build());
        streams.setStateListener((newState, oldState) -> System.out.println("*** Changed state from " +oldState + " to " + newState));
        streams.start();

        if (scale > 1) {
            for (var threads = 1; threads < scale; threads++) {
                logger.info(String.format("Increased thread count to %d", threads));
                streams.addStreamThread();
            }
        }

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