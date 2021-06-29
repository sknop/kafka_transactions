package admin;

import common.AbstractBase;
import org.apache.kafka.clients.admin.*;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

@CommandLine.Command(name = "Admin",
        version = "Admin 1.0",
        description = "Starting point for an admin swiss army knife.")
public class Admin extends AbstractBase implements Callable<Integer> {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @CommandLine.Option(names = {"--bootstrap-servers"})
    protected String bootstrapServers;

    @CommandLine.Option(names = {"--create"})
    protected String topicToCreate;

    @CommandLine.Option(names = {"--partitions"})
    protected int numberOfPartitions = 1;

    @CommandLine.Option(names = {"--replication-factor"})
    protected short replicationFactor = 1;

    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out all topics")
    protected boolean verbose = false;

    private AdminClient client;

    public Admin() {
    }

    @Override
    protected void createProperties() {
        Properties properties = new Properties();

        readConfigFile(properties);

        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        client = AdminClient.create(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            client.close();
        }));
    }

    public List<String> getTopics() throws InterruptedException, ExecutionException {
        var topics = client.listTopics();

        var listings = topics.namesToListings();
        return new ArrayList<>(listings.get().keySet());
    }

    public Map<String,TopicDescription> getDescription(List<String> topics) throws InterruptedException, ExecutionException {
        var descriptions = client.describeTopics(topics).all();

        return descriptions.get();
    }

    public CreateTopicsResult createTopic() throws ExecutionException, InterruptedException {
        var newTopic = new NewTopic(topicToCreate, numberOfPartitions, replicationFactor);

        return client.createTopics(Collections.singletonList(newTopic));
    }

    @Override
    public Integer call() {
        createProperties();

        List<String> topics;
        try {
            var clusterDescription = client.describeCluster();
            System.out.println("ClusterID : " + clusterDescription.clusterId().get());
            var controller = clusterDescription.controller().get();

            for (var node : clusterDescription.nodes().get()) {
                String output = String.format("Id = %d %s:%d",node.id(), node.host(), node.port());
                if (node == controller) {
                    output += " Controller";
                }
                System.out.println(output);
            }

            if (verbose) {
                topics = getTopics();
                topics.forEach(System.out::println);

                var descriptions = getDescription(topics);
                descriptions.values().forEach(System.out::println);
            }

            if (topicToCreate != null) {
                var config = createTopic();
                System.out.println(config.config(topicToCreate).get());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new Admin()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
