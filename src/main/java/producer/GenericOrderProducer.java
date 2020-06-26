package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

@CommandLine.Command(name = "GenericOrderProducer",
        mixinStandardHelpOptions = true,
        version = "GenericOrderProducer 1.0",
        description = "Produces Producer objects in Avro format, using the GenericRecord, either a fixed amount or continuously.",
        sortOptions = false)
public class GenericOrderProducer implements Callable<Integer> {
    private static final String DEFAULT_TOPIC = "order";

    @CommandLine.Option(names = {"--bootstrap-servers"},
            description = "Bootstrap Servers (default = ${DEFAULT-VALUE})",
            defaultValue = "localhost:9092")
    private String bootstrapServers;

    @CommandLine.Option(names = {"--schema-registry"},
            description = "Schema Registry (default = ${DEFAULT-VALUE})",
            defaultValue = "http://localhost:8081")
    private String schemaRegistryURL;

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})",
            defaultValue = DEFAULT_TOPIC)
    private String topic;

    @CommandLine.Option(names = {"-m", "--max"},
            description = "Max numbers of objects to generate/update (default = ${DEFAULT-VALUE}, keep going)")
    private int maxObjects = -1;

    @CommandLine.Option(names = {"-l", "--largest-id"},
            description = "Highest ID to generate/update (default = ${DEFAULT-VALUE})")
    private int largestId = 1000;

    @CommandLine.Option(names = {"-i", "--interactive"},
            description = "If enabled, will produce one event and wait for <Return>")
    private boolean interactive;

    @CommandLine.Option(names = {"-c", "--config-file"},
            description = "If provided, content will be added to the properties")
    private String configFile = null;

    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    private boolean verbose = false;

    @CommandLine.Option(names = {"--avro-schema-file"},
            description = "Avro schema file for the order schema",
            required = true)
    private String avroSchemaFile;

    private boolean doProduce = true;
    private int produced = 0;
    private Random random = new Random();

    private Schema schema;

    public GenericOrderProducer() {
    }

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

        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        properties.put("confluent.monitoring.interceptor.bootstrap.servers", bootstrapServers);
        properties.put("confluent.monitoring.interceptor.timeout.ms", 3000);
        properties.put("confluent.monitoring.interceptor.publishMs", 10000);

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

        if (maxObjects == -1) {
            while (doProduce) {
                doProduce(producer);

                if (interactive) {
                    System.out.println("Press any key to continue ...");

                    var c = reader.read();
                }
            }
        } else {
            for (int i = 0; i < maxObjects; i++) {
                doProduce(producer);
                if (interactive) {
                    System.out.println("Press any key to continue ...");

                    var c = reader.read();
                }
            }
        }
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
            System.out.println("Produced [" + valueSize + "] " + record);
        produced++;

    }

    private ProducerRecord<Integer, Object> createRecord() {
        GenericRecord record = new GenericData.Record(schema);

        int orderId = random.nextInt(largestId) + 1;
        int customerId = random.nextInt(largestId) + 1;
        int orderAmount = random.nextInt(largestId) + 1;

        Long date = System.currentTimeMillis();

        record.put("orderId", orderId);
        record.put("date", date);
        record.put("orderAmount", customerId);
        record.put("customerId", Integer.toString(customerId));

        return new ProducerRecord<>(topic, orderId, record);
    }

    private void importSchema() {
        var contentBuilder = new StringBuilder();

        try (Stream<String> stream = Files.lines(Paths.get(avroSchemaFile), StandardCharsets.UTF_8)) {
            stream.forEach((s -> contentBuilder.append(s).append("\n")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.err.println("Inputfile " + avroSchemaFile + " not found");
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(contentBuilder.toString());
    }

    @Override
    public Integer call() throws Exception {
        importSchema();
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new GenericOrderProducer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
