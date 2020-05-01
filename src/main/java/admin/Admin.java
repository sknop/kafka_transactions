package admin;

import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Admin {
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String bootstrapServers;
    private String schemaRegistryURL;

    private AdminClient client;

    public Admin(Namespace options) {
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");

        Map<String, Object> conf = new HashMap<>();
        conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        client = AdminClient.create(conf);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            client.close();
        }));
    }

    public List<String> getTopics() {
        var topics = client.listTopics();

        var listings = topics.namesToListings();
        var result = new ArrayList<String>();

        try {
            result.addAll(listings.get().keySet());
        } catch (InterruptedException e) {
            System.err.println("I've been rudely interrupted");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.err.println("Something went wrong");
            e.printStackTrace();
        }

        return result;
    }

    public Map<String,TopicDescription> getDescription(List<String> topics) {
        var descriptions = client.describeTopics(topics).all();

        try {
            return descriptions.get();
        } catch (InterruptedException e) {
            System.err.println("I've been rudely interrupted");
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.err.println("Something went wrong");
            e.printStackTrace();
        }

        return new HashMap<>();
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("Admin").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--bootstrap-servers")
                .type(String.class)
                .setDefault(BOOTSTRAP_SERVERS)
                .help(String.format("Kafka Bootstrap Servers(default %s)", BOOTSTRAP_SERVERS));
        parser.addArgument("--schema-registry")
                .type(String.class)
                .setDefault(SCHEMA_REGISTRY_URL)
                .help(String.format("Schema registry URL(de fault %s)", SCHEMA_REGISTRY_URL));
        try {
            Namespace options = parser.parseArgs(args);

            Admin admin = new Admin(options);

            var topics = admin.getTopics();
            topics.forEach(System.out::println);
            var descriptions = admin.getDescription(topics);
            descriptions.values().forEach(System.out::println);

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
