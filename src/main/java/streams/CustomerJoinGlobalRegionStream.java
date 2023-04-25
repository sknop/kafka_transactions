package streams;

import common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import picocli.CommandLine;
import schema.Customer;
import schema.CustomerWithRegion;
import schema.Region;

import java.util.Properties;

@CommandLine.Command(name = "CustomerJoinGlobalRegionStream",
        version = "CustomerJoinGlobalRegionStream 1.0",
        description = "Reads Customer objects in Avro format from a stream.")
public class CustomerJoinGlobalRegionStream extends AbstreamStream {
    final static String CUSTOMER_TOPIC = "customer";
    final static String REGION_TOPIC = "region";
    final static String CUSTOMER_WITH_GLOBAL_REGION_TOPIC = "customer-with-global-region";
    final static String REGION_GLOBAL_TABLE = "regions-global-table";

    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String customerTopic = CUSTOMER_TOPIC;

    @CommandLine.Option(names = {"--region-topic"},
            description = "Topic for the region (default = ${DEFAULT-VALUE})")
    private String regionTopic = REGION_TOPIC;

    @CommandLine.Option(names = {"--customer-with-global-region-topic"},
            description = "Topic for the customer-with-region (default = ${DEFAULT-VALUE})")
    private String customerWithGlobalRegion = CUSTOMER_WITH_GLOBAL_REGION_TOPIC;

    public CustomerJoinGlobalRegionStream() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        // pass
    }

    @Override
    protected String getApplicationName() {
        return "customer-join-region-stream";
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        var regions = builder.globalTable(regionTopic,
                Materialized.<String, Region, KeyValueStore<Bytes, byte[]>>as(REGION_GLOBAL_TABLE).
                        withKeySerde(Serdes.String()).
                        withValueSerde(SerdeGenerator.getSerde(properties)));

        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic,
                Consumed.with(Serdes.Integer(), SerdeGenerator.getSerde(properties))
        );

        ValueJoiner<Customer, Region, CustomerWithRegion> valueJoiner =
                (customer, region) -> CustomerWithRegion.newBuilder()
                        .setCustomerId(customer.getCustomerId())
                        .setFirstName(customer.getFirstName())
                        .setLastName(customer.getLastName())
                        .setEmail(customer.getEmail())
                        .setAge(customer.getAge())
                        .setRegionName(region.getLongName())
                        .setAreaCode(region.getAreaCode()).build();

        KeyValueMapper<Integer, Customer, String> keyValueMapper = (key, customer) -> customer.getRegion();

        existingCustomers.join(regions, keyValueMapper, valueJoiner)
                .peek((k,v) -> System.out.println("Peeked " + k + " with value " + v))
                .to(customerWithGlobalRegion, Produced.with(Serdes.Integer(), SerdeGenerator.getSerde(properties)));
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new CustomerJoinGlobalRegionStream()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}