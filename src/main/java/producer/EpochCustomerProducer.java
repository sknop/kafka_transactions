package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import schema.Customer;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;

public class EpochCustomerProducer {
    final static String CUSTOMER_TOPIC = "customer";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String customerTopic;
    private int maxCustomers;

    private String bootstrapServers;
    private String schemaRegistryURL;

    private Random random = new Random();
    private boolean doProduce = false;

    public EpochCustomerProducer(Namespace options) {
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

        return new KafkaProducer<>(properties);
    }

    private KafkaStreams createStreams(Topology topology) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "epoch-customer-producer");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Specify default (de)serializers for record keys and for record values.
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        return new KafkaStreams(topology, properties);
    }

    private void produce() {
        KafkaProducer<Integer, Object> producer = createProducer();
        StreamsBuilder builder = new StreamsBuilder();

//        final Map<String, String> serdeConfig =
//                Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
//        final Serde<Customer> customerSerde = new SpecificAvroSerde<>();
//        customerSerde.configure(serdeConfig, false)

        KTable<Integer, Customer> existingCustomers = builder
                .table(customerTopic, Materialized.<Integer, Customer, KeyValueStore<Bytes, byte[]>>as("customer-store"));
        KafkaStreams streams = createStreams(builder.build());

        streams.setStateListener((newState, oldState) -> {
            System.out.println("*** State transition from " + oldState + " to " + newState);

            if (KafkaStreams.State.REBALANCING == oldState && KafkaStreams.State.RUNNING == newState) {
                System.out.println("State store ready, off we go!");
                doProduce = true;
                produceWithStore(producer, existingCustomers, streams);
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            doProduce = false;
            producer.close();
            streams.close();
        }));

        streams.start();
    }

    private void produceWithStore(KafkaProducer<Integer, Object> producer, KTable<Integer, Customer> existingCustomers, KafkaStreams streams) {
        String storeName = existingCustomers.queryableStoreName();
        ReadOnlyKeyValueStore<Integer, Customer> keyValueStore = streams.store(storeName, QueryableStoreTypes.keyValueStore());

        while(doProduce) {
            ProducerRecord<Integer, Object> record = createRecord(keyValueStore);

            producer.send(record);
            producer.flush();

            // System.out.println("Produced " + record);

        }
    }

    private ProducerRecord<Integer,Object> createRecord(ReadOnlyKeyValueStore<Integer, Customer> keyValueStore) {
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

        int customerId = random.nextInt(maxCustomers) + 1;

        Customer probe = keyValueStore.get(customerId);
        if (probe != null) {
            probe.setEpoch(probe.getEpoch() + 1);
            probe.setUpdate(date);

            return new ProducerRecord<>(customerTopic, probe.getCustomerId(), probe);
        }
        else {
            String firstName = "first_" + customerId;
            String lastName = "last_" + customerId;
            String email = "email_" + customerId + "@email.com";

            int epoch = 1;

            Customer customer = new Customer(customerId, firstName, lastName, email, date, epoch);

            return new ProducerRecord<>(customerTopic, customerId, customer);
        }
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("EpochCustomerProducer").addHelp(true);

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

            EpochCustomerProducer producer = new EpochCustomerProducer(options);
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

