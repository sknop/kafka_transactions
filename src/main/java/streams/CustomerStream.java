package streams;

import common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import picocli.CommandLine;
import schema.Customer;

import java.util.Properties;

@CommandLine.Command(name = "CustomerStream",
        version = "CustomerStream 1.0",
        description = "Reads Customer objects in Avro format from a stream.")
public class CustomerStream extends AbstractStream {
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
        // properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        // properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic, Consumed.with(Serdes.Integer(), SerdeGenerator.getSerde(properties)));

        if (verbose)
            existingCustomers.
                    filter((key, value) -> value.getAge() > 30 && value.getAge() < 55).
                    foreach((key, value) -> System.out.println(key + " => " + value));
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