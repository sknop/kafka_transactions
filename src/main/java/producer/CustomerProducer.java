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
import org.apache.kafka.streams.kstream.KTable;
import schema.Customer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class CustomerProducer {
    final static String CUSTOMER_TOPIC = "customer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String customerTopic;
    private int maxCustomers;

    private String bootstrapServers;
    private String schemaRegistryURL;

    private Random random = new Random();

    public CustomerProducer(Namespace options) {
        customerTopic = options.get("customer_topic");
        maxCustomers = options.get("max_customers");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    private KafkaProducer<Integer, Object> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        KafkaProducer<Integer, Object> producer = new KafkaProducer<>(properties);

        return producer;
    }

    private void produce() {
        KafkaProducer<Integer, Object> producer = createProducer();
        // KTable<Integer, Object> customers =


        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            mainThread.interrupt();
        }));

        while(true) {
            ProducerRecord<Integer, Object> record = createRecord(producer);

            producer.send(record);

            System.out.println("Produced " + record);

            if(mainThread.isInterrupted()) {
               producer.flush();
               producer.close();

               return;
            }

        }
    }

    private ProducerRecord<Integer,Object> createRecord(KafkaProducer<Integer,Object> producer) {
        // create a random number up to max_customers
        // search db for that number
        // if found:
        //  recreate object in memory
        //  update generation (email address)
        // else:
        //  create new object
        // save object
        // produce and return record0

        int customerId = random.nextInt(maxCustomers) + 1;
        String firstName = "first_" + customerId;
        String lastName = "last_" + customerId;
        String email = "email_" + customerId + "@email.com";

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        String date = dtf.format(now);
        int epoch = 1;

        Customer customer = new Customer(customerId, firstName, lastName, email, date, epoch);

        return new ProducerRecord<>(customerTopic, customerId, customer);
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("TransactionalProducer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("-m", "--max-customers")
                .setDefault(1000)
                .type(Integer.class)
                .help("Max numbers of users to generate/update");
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
        }
    }

}

