package streams;

import common.SerdeGenerator;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import picocli.CommandLine;
import schema.Customer;
import schema.CustomerWithRegion;
import schema.Region;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

@CommandLine.Command(name = "CustomerJoinRegionStream",
        version = "CustomerJoinRegionStream 1.0",
        description = "Reads Customer objects in Avro format from a stream.")
public class CustomerJoinRegionStream extends AbstreamStream {
    final static String CUSTOMER_TOPIC = "customer";
    final static String REGION_TOPIC = "region";

    final static String CUSTOMER_WITH_REGION_TOPIC = "customer-with-region";

    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String customerTopic = CUSTOMER_TOPIC;

    @CommandLine.Option(names = {"--region-topic"},
            description = "Topic for the region (default = ${DEFAULT-VALUE})")
    private String regionTopic = REGION_TOPIC;

    @CommandLine.Option(names = {"--customer-with-region-topic"},
            description = "Topic for the customer-with-region (default = ${DEFAULT-VALUE})")
    private String customerWithRegion = CUSTOMER_WITH_REGION_TOPIC;

    public CustomerJoinRegionStream() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        // pass
    }

    private List<Integer> getPartitions() {
        try(Admin adminClient = Admin.create(properties)) {
            var results = adminClient.describeTopics(Arrays.asList(regionTopic, customerTopic));
            var topicValues = results.topicNameValues();

            var regionPartitions = topicValues.get(regionTopic).get().partitions().size();
            var customerPartitions = topicValues.get(customerTopic).get().partitions().size();

            return Arrays.asList(regionPartitions, customerPartitions);
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getApplicationName() {
        return "customer-join-region-stream";
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        var partitionCount = getPartitions();

        int regionPartitions = partitionCount.get(0);
        int customerPartitions = partitionCount.get(1);

        KTable<String, Region> regions = builder.table(regionTopic,
                Consumed.with(Serdes.String(), SerdeGenerator.getSerde(properties)),
                Materialized.as("regions-table"));

        KStream<Integer, Customer> existingCustomers = builder.stream(customerTopic,
                Consumed.with(Serdes.Integer(), SerdeGenerator.getSerde(properties))
        );

        var repartitioned = Repartitioned.with(Serdes.String(), SerdeGenerator.<Customer>getSerde(properties))
                .withName("customer-by-region");

        if (regionPartitions != customerPartitions) {
            repartitioned.withNumberOfPartitions(regionPartitions);
        }

        ValueJoiner<Customer, Region, CustomerWithRegion> valueJoiner =
                (customer, region) -> new CustomerWithRegion(
                        customer.getCustomerId(),
                        customer.getFirstName(),
                        customer.getLastName(),
                        customer.getEmail(),
                        customer.getAge(),
                        region.getLongName(),
                        region.getAreaCode());

        var joiner = Joined.with(Serdes.String(), SerdeGenerator.<Customer>getSerde(properties), SerdeGenerator.<Region>getSerde(properties));

        existingCustomers.selectKey((k, v) -> v.getRegion(), Named.as("customer-by-region"))
                .repartition(repartitioned)
                .join(regions, valueJoiner, joiner)
                .to(customerWithRegion, Produced.with(Serdes.String(), SerdeGenerator.getSerde(properties)));
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new CustomerJoinRegionStream()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}