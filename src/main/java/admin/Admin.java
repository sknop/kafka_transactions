package admin;

import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Admin {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String bootstrapServers;
    private String schemaRegistryURL;
    private String configFile;

    private AdminClient client;

    public Admin(Namespace options) {
        Properties properties = new Properties();

        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
        configFile = options.get("config_file");

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

    public Config createTopic(String name, int numberOfPartitions, short replicationFactor) throws ExecutionException, InterruptedException {
        var newTopic = new NewTopic(name, numberOfPartitions, replicationFactor);

        var results = client.createTopics(Collections.singletonList(newTopic));
        return results.config(name).get();
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("Admin").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--config-file")
                .type(String.class)
                .help("Config file for authentication, for example");
        parser.addArgument("--bootstrap-servers")
                .type(String.class)
                .setDefault(BOOTSTRAP_SERVERS)
                .help(String.format("Kafka Bootstrap Servers(default %s)", BOOTSTRAP_SERVERS));
        parser.addArgument("--schema-registry")
                .type(String.class)
                .setDefault(SCHEMA_REGISTRY_URL)
                .help(String.format("Schema registry URL(de fault %s)", SCHEMA_REGISTRY_URL));
        parser.addArgument("-c","--create")
                .type(String.class)
                .help("Create a topic <name>");
        parser.addArgument("-p","--partitions")
                .type(Integer.class)
                .setDefault(1)
                .help("Topic number of partitions");
        parser.addArgument("-r","--replication")
                .type(Short.class)
                .setDefault((short)1)
                .help("Topic replication-factor");
        try {
            Namespace options = parser.parseArgs(args);

            Admin admin = new Admin(options);

            var topics = admin.getTopics();
            topics.forEach(System.out::println);
            var descriptions = admin.getDescription(topics);
            descriptions.values().forEach(System.out::println);

            String topicToCreate = options.get("create");
            if (topicToCreate != null) {
                int partitions = options.get("partitions");
                short replicationFactor = options.get("replication");

                var config = admin.createTopic(topicToCreate, partitions, replicationFactor);
                System.out.println(config);
            }

        } catch (ArgumentParserException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(parser.formatHelp());
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
