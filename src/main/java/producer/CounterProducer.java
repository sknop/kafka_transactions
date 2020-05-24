package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.RecordNameStrategy;
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
import schema.Counter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class CounterProducer {
    final static String COUNTER_TOPIC = "counter";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final int DEFAULT_MAX_COUNTER = 10000;
    public static final int DEFAULT_GAP = 1000;

    private String counterTopic;
    private int maxCounter;
    private int largestCounterId;

    private String bootstrapServers;
    private String schemaRegistryURL;

    private boolean interactive;
    private boolean doProduce = true;
    private boolean verbose = false;

    private int counter = 0;

    private LinkedList<Integer> counters = new LinkedList<>();
    private int gap;

    public CounterProducer(Namespace options) {
        counterTopic = options.get("counter_topic");
        maxCounter = options.get("max_counters");
        largestCounterId = options.get("largest_counterid");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
        interactive = options.get("interactive");
        verbose = options.get("verbose");
        gap = options.get("gap");
        counter = options.get("offset");
    }

    private KafkaProducer<Integer, Object> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        // properties.put("confluent.monitoring.interceptor.bootstrap.servers", bootstrapServers);
        // properties.put("confluent.monitoring.interceptor.timeout.ms", 3000);
        // properties.put("confluent.monitoring.interceptor.publishMs", 10000);

        return new KafkaProducer<>(properties);
    }

    private void produce() {
        KafkaProducer<Integer, Object> producer = createProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            doProduce = false;
            producer.close();
        }));

        if (maxCounter == -1) {
            while (doProduce) {
                doProduce(producer);

                if (interactive) {
                    System.out.println("Press return for next ...");

                    try {
                        int key = System.in.read();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } else {
            for (int i = 0; i < maxCounter; i++) {
                doProduce(producer);
                if (i > gap) {
                    doProduceTombstone(producer);
                }
            }
        }

        System.out.println("Total produced = " + counter);
    }

    private void doProduceTombstone(KafkaProducer<Integer, Object> producer) {
        int removeCounter = counters.removeFirst();

        ProducerRecord<Integer, Object> tombstone = new ProducerRecord<>(counterTopic, removeCounter, null);
        int valueSize = produceRecord(producer, tombstone);

        if (verbose)
            System.out.println("Produced Tombstone [" + valueSize + "] " + tombstone);
    }

    private void doProduce(KafkaProducer<Integer, Object> producer) {
        ProducerRecord<Integer, Object> record = createRecord();
        int valueSize = produceRecord(producer, record);

        if (verbose)
            System.out.println("Produced [" + valueSize + "] " + record);
    }

    private int produceRecord(KafkaProducer<Integer, Object> producer, ProducerRecord<Integer, Object> tombstone) {
        int valueSize = 0;

        Future<RecordMetadata> future = producer.send(tombstone);
        try {
            RecordMetadata result = future.get();
            valueSize = result.serializedValueSize();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        producer.flush();

        return valueSize;
    }

    private ProducerRecord<Integer, Object> createRecord() {
        counter += 1;
        long timestamp = System.currentTimeMillis();

        Counter counterObject = new Counter(counter, timestamp);
        counters.add(counter);

        return new ProducerRecord<>(counterTopic, counter, counterObject);
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("CounterProducer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("-m", "--max-counters")
                .setDefault(-1)
                .type(Integer.class)
                .help("Max numbers of counters to generate/update (default = -1, keep going)");
        parser.addArgument("-l", "--largest-counterid")
                .setDefault(DEFAULT_MAX_COUNTER)
                .type(Integer.class)
                .help("Highest counter ID to generate/update");
        parser.addArgument("-g", "--gap")
                .setDefault(DEFAULT_GAP)
                .type(Integer.class)
                .help("Gap between produce and delete");
        parser.addArgument("-o", "--offset")
                .setDefault(0)
                .type(Integer.class)
                .help("Offset from where to start the counter");
        parser.addArgument("--counter-topic")
                .type(String.class)
                .setDefault(COUNTER_TOPIC)
                .help(String.format("Topic for the counter (default %s)", COUNTER_TOPIC));
        parser.addArgument("--bootstrap-servers")
                .type(String.class)
                .setDefault(BOOTSTRAP_SERVERS)
                .help(String.format("Kafka Bootstrap Servers(default %s)", BOOTSTRAP_SERVERS));
        parser.addArgument("--schema-registry")
                .type(String.class)
                .setDefault(SCHEMA_REGISTRY_URL)
                .help(String.format("Schema registry URL(de fault %s)", SCHEMA_REGISTRY_URL));
        parser.addArgument("-i", "--interactive")
                .action(Arguments.storeConst())
                .setDefault(Boolean.FALSE)
                .setConst(Boolean.TRUE)
                .help("If enabled, will produce one event and wait for <Return>");
        parser.addArgument("-v", "--verbose")
                .action(Arguments.storeConst())
                .setDefault(Boolean.FALSE)
                .setConst(Boolean.TRUE)
                .help("If enabled, will print out every message created");

        try {
            Namespace options = parser.parseArgs(args);

            CounterProducer producer = new CounterProducer(options);
            producer.produce();

        } catch (ArgumentParserException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(parser.formatHelp());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

