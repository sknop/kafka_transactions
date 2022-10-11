package streams;

import common.SerdeGenerator;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import picocli.CommandLine;
import schema.Customer;
import schema.CustomerWithRegion;
import schema.Region;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

@CommandLine.Command(name = "CustomerJoinRegionStream",
        version = "CustomerJoinRegionStream 1.0",
        description = "Reads Customer objects in Avro format from a stream.")
public class CustomerJoinRegionStream extends AbstreamStream implements Callable<Integer> {
    final static String CUSTOMER_TOPIC = "customer";
    final static String REGION_TOPIC = "region";

    final static String CUSTOMER_WITH_REGION_TOPIC = "customer-with-region";

    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String customerTopic = CUSTOMER_TOPIC;

    @CommandLine.Option(names = {"--region-topic"},
            description = "Topic for the region (default = ${DEFAULT-VALUE})")
    private String regionTopic = REGION_TOPIC;

    @CommandLine.Option(names = {"--region-with-region-topic"},
            description = "Topic for the customer-with-region (default = ${DEFAULT-VALUE})")
    private String customerWithRegion = CUSTOMER_WITH_REGION_TOPIC;

    public CustomerJoinRegionStream() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        // pass
    }

    @Override
    protected String getApplicationName() {
        return "customer-join-region-stream";
    }

    private void consume() {
        createProperties();

        int regionPartitions = 0;
        int customerPartitions = 0;

        try(Admin adminClient = Admin.create(properties)) {
            var results = adminClient.describeTopics(Arrays.asList(regionTopic, customerTopic));
            var topicValues = results.topicNameValues();
            for (var entry : topicValues.entrySet()) {
                if (entry.getKey().equals(regionTopic)) {
                    var topicDescription = entry.getValue().get();
                    regionPartitions = topicDescription.partitions().size();
                }
                if (entry.getKey().equals(customerTopic)) {
                    var topicDescription = entry.getValue().get();
                    customerPartitions = topicDescription.partitions().size();
                }
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        StreamsBuilder builder = new StreamsBuilder();

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
                .to(customerWithRegion, Produced.with(Serdes.String(), SerdeGenerator.<CustomerWithRegion>getSerde(properties)));

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
            new CommandLine(new CustomerJoinRegionStream()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}