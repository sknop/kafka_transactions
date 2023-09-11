package streams;

import common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import picocli.CommandLine;
import schema.Customer;
import schema.CustomerWithRegion;
import schema.Region;

import java.util.Properties;

@CommandLine.Command(name = "CustomerJoinNoRepartitionRegionStream",
        version = "CustomerJoinNoRepartitionRegionStream 1.0",
        description = "Reads Customer objects in Avro format from a stream.")
    public class CustomerJoinRegionNoRepartitionStream extends AbstreamStream {
    final static String CUSTOMER_TOPIC = "customer";
    final static String REGION_TOPIC = "region";

    final static String CUSTOMER_WITH_REGION_TOPIC = "customer-with-region-by-id";

    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String customerTopic = CUSTOMER_TOPIC;

    @CommandLine.Option(names = {"--region-topic"},
            description = "Topic for the region (default = ${DEFAULT-VALUE})")
    private String regionTopic = REGION_TOPIC;

    @CommandLine.Option(names = {"--customer-with-region-topic"},
            description = "Topic for the customer-with-region (default = ${DEFAULT-VALUE})")
    private String customerWithRegion = CUSTOMER_WITH_REGION_TOPIC;

    public CustomerJoinRegionNoRepartitionStream() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SerdeGenerator.<Customer>getSerde(properties).getClass().getName());
    }

    @Override
    protected String getApplicationName() {
        return "customer-join-region-stream";
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {

        KTable<String, Region> regions = builder.table(regionTopic,
                Consumed.with(Serdes.String(), SerdeGenerator.getSerde(properties)),
                Materialized.as("regions-table"));

        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic,
                Consumed.with(Serdes.Integer(), SerdeGenerator.getSerde(properties))
        );


//        ValueJoiner<Customer, Region, CustomerWithRegion> valueJoiner =
//                (customer, region) -> new CustomerWithRegion(
//                        customer.getCustomerId(),
//                        customer.getFirstName(),
//                        customer.getLastName(),
//                        customer.getEmail(),
//                        customer.getAge(),
//                        region.getLongName(),
//                        region.getAreaCode());

//        var joiner = Joined.with(Serdes.String(), SerdeGenerator.<Customer>getSerde(properties), SerdeGenerator.<Region>getSerde(properties));

        existingCustomers.selectKey((key, value) -> value.getRegion(), Named.as("customer-by-region")).
            leftJoin(regions, (customer, region) -> {
                if (region == null) {
                    return null;
                }
                return new CustomerWithRegion(
                        customer.getCustomerId(),
                        customer.getFirstName(),
                        customer.getLastName(),
                        customer.getEmail(),
                        customer.getAge(),
                        region.getLongName(),
                        region.getAreaCode());
            })
            .peek((key, value) -> System.out.println("Creating " + value))
            .selectKey((key,value) -> value.getCustomerId())
            .to(customerWithRegion, Produced.with(Serdes.Integer(), SerdeGenerator.getSerde(properties)));
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new CustomerJoinRegionNoRepartitionStream()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}