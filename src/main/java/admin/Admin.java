package admin;

import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.*;
import picocli.CommandLine;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

@CommandLine.Command(
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false)
public class Admin implements Callable<Integer> {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    @CommandLine.Option(names = {"--bootstrap-servers"})
    protected String bootstrapServers;

    @CommandLine.Option(names = {"-c", "--config-file"},
            description = "If provided, content will be added to the properties")
    protected String configFile = null;

    @CommandLine.Option(names = {"--create"})
    protected String topicToCreate;

    @CommandLine.Option(names = {"--partitions"})
    protected int numberOfPartitions = 1;

    @CommandLine.Option(names = {"--replication-factor"})
    protected short replicationFactor = 1;

    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    protected boolean verbose = false;

    private AdminClient client;

    public Admin() {
    }

    private void initialize() {
        Properties properties = new Properties();

        if (configFile != null) {
            try (InputStream inputStream = new FileInputStream(configFile)) {
                Reader reader = new InputStreamReader(inputStream);

                properties.load(reader);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.err.println("Inputfile " + configFile + " not found");
                System.exit(1);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

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

    public Config createTopic() throws ExecutionException, InterruptedException {
        var newTopic = new NewTopic(topicToCreate, numberOfPartitions, replicationFactor);

        var results = client.createTopics(Collections.singletonList(newTopic));
        return results.config(topicToCreate).get();
    }

    @Override
    public Integer call() {
        initialize();

        List<String> topics = null;
        try {
            if (verbose) {
                topics = getTopics();
                topics.forEach(System.out::println);

                var descriptions = getDescription(topics);
                descriptions.values().forEach(System.out::println);
            }

            if (topicToCreate != null) {
                var config = createTopic();
                System.out.println(config);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
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
