package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import schema.Customer;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class CustomerProducer {
    final static String CUSTOMER_TOPIC = "customer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String customerTopic;
    private int maxCustomers;
    private int largestCustomerId;

    private String bootstrapServers;
    private String schemaRegistryURL;

    private Random random = new Random();

    public CustomerProducer(Namespace options) {
        customerTopic = options.get("customer_topic");
        maxCustomers = options.get("max_customers");
        largestCustomerId = options.get("largest_customerid");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    private KafkaProducer<Integer, Object> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");

        return new KafkaProducer<>(properties);
    }

    private void produce() {
        KafkaProducer<Integer, Object> producer = createProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            producer.close();
        }));

        if (maxCustomers == -1) {
            while (true) {
                doProduce(producer);
                System.out.println("Press return for next ...");

                try {
                    int key = System.in.read();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        else {
            for (int i = 0; i < maxCustomers; i++) {
                doProduce(producer);
            }
        }
    }

    private void doProduce(KafkaProducer<Integer, Object> producer) {
        ProducerRecord<Integer, Object> record = createRecord();

        producer.send(record);
        producer.flush();

        System.out.println("Produced " + record);
    }
    private ProducerRecord<Integer,Object> createRecord() {
        // create a random number up to max_customers
        // search db for that number
        // if found:
        //  recreate object in memory
        //  update generation (email address)
        // else:
        //  create new object
        // save object
        // produce and return record0

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss xxx");
        ZonedDateTime now = ZonedDateTime.now();

        String date = dtf.format(now);

        int customerId = random.nextInt(largestCustomerId) + 1;

        String firstName = "first_" + customerId;
        String lastName = "last_" + customerId;
        String email = "email_" + customerId + "@email.com";

        int epoch = 1;

        Customer customer = new Customer(customerId, firstName, lastName, email, date, epoch);

        return new ProducerRecord<>(customerTopic, customerId, customer);
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("CustomerProducer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("-m", "--max-customers")
                .setDefault(-1)
                .type(Integer.class)
                .help("Max numbers of users to generate/update (default = -1, keep going)");
        parser.addArgument("-l", "--largest-customerid")
                .setDefault(1000)
                .type(Integer.class)
                .help("Highest customer ID to generate/update");
        parser.addArgument("--customer-topic")
                .type(String.class)
                .setDefault(CUSTOMER_TOPIC)
                .help(String.format("Topic for the customer (default %s)",CUSTOMER_TOPIC));
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

            CustomerProducer producer = new CustomerProducer(options);
            producer.produce();

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

