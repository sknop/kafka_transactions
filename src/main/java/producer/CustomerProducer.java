package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
// import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import schema.Customer;

import java.io.*;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@CommandLine.Command(name = "CustomerProducer",
        mixinStandardHelpOptions = true,
        version = "CustomerProducer 1.0",
        description = "Produces Customer objects in Avro format, either a fixed amount or continuously.",
        sortOptions = false)
public class CustomerProducer implements Callable<Integer> {

    @CommandLine.Option(names = {"--bootstrap-servers"},
            description = "Bootstrap Servers (default = ${DEFAULT-VALUE})",
            defaultValue = "localhost:9092")
    private String bootstrapServers;

    @CommandLine.Option(names = {"--schema-registry"},
            description = "Schema Registry (default = ${DEFAULT-VALUE})",
            defaultValue = "http://localhost:8081")
    private String schemaRegistryURL;

    @CommandLine.Option(names = {"--customer-topic"},
                        description = "Topic for the customer (default = ${DEFAULT-VALUE})",
                        defaultValue = "customer")
    private String customerTopic;

    @CommandLine.Option(names = {"-m", "--max-customers"},
        description = "Max numbers of users to generate/update (default = ${DEFAULT-VALUE}, keep going)")
    private int maxCustomers = -1;

    @CommandLine.Option(names = {"-l", "--largest-customerid"},
                        description = "Highest customer ID to generate/update (default = ${DEFAULT-VALUE})")
    private int largestCustomerId = 1000;

    @CommandLine.Option(names = {"-i", "--interactive"},
                        description = "If enabled, will produce one event and wait for <Return>")
    private boolean interactive;

    @CommandLine.Option(names = {"-c", "--config-file"},
                        description = "If provided, content will be added to the properties")
    private String configFile = null;

    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    private boolean verbose = false;

    private boolean doProduce = true;
    private int produced = 0;
    private Random random = new Random();

    public CustomerProducer() {  }

    private KafkaProducer<Integer, Object> createProducer() {
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

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
//        properties.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,false);

//        properties.put(KafkaAvroSerializerConfig.USER_INFO_CONFIG,"alice:alice-secret");
//        properties.put(KafkaAvroSerializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        properties.put("confluent.monitoring.interceptor.bootstrap.servers", bootstrapServers);
        properties.put("confluent.monitoring.interceptor.timeout.ms", 3000);
        properties.put("confluent.monitoring.interceptor.publishMs", 10000);

        // properties.load(Reader.nullReader())
        return new KafkaProducer<>(properties);
    }

    private void produce() throws IOException {
        KafkaProducer<Integer, Object> producer = createProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            doProduce = false;
            producer.close();
        }));

        var terminal = TerminalBuilder.terminal();
        terminal.enterRawMode();
        var reader = terminal.reader();

        if (maxCustomers == -1) {
            while (doProduce) {
                doProduce(producer);

                if (interactive) {
                    System.out.println("Press any key to continue ...");

                    var c = reader.read();
                }
            }
        }
        else {
            for (int i = 0; i < maxCustomers; i++) {
                doProduce(producer);
                if (interactive) {
                    System.out.println("Press any key to continue ...");

                    var c = reader.read();
                }
            }
        }

        System.out.println("Total produced = " + produced);
    }

    private void doProduce(KafkaProducer<Integer, Object> producer) {
        ProducerRecord<Integer, Object> record = createRecord();
        int valueSize = 0;

        Future<RecordMetadata> future = producer.send(record);
        try {
            RecordMetadata result = future.get();
            valueSize = result.serializedValueSize();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        producer.flush();

        if (verbose)
            System.out.println("Produced [" +valueSize + "] " + record);
        produced++;
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

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new CustomerProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

