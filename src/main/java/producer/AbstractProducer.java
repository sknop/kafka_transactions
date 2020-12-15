package producer;

import common.AbstractBase;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;

import java.io.*;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@CommandLine.Command(
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false)
public abstract class AbstractProducer extends AbstractBase {

    @CommandLine.Option(names = {"--bootstrap-servers"})
    protected String bootstrapServers;

    @CommandLine.Option(names = {"--schema-registry"})
    protected String schemaRegistryURL;

    @CommandLine.Option(names = {"-m", "--max"},
            description = "Max numbers of objects to generate/update (default = ${DEFAULT-VALUE}, keep going)")
    protected int maxObjects = -1;

    @CommandLine.Option(names = {"-l", "--largest"},
            description = "Highest object ID to generate/update (default = ${DEFAULT-VALUE})")
    protected int largestId = 1000;

    @CommandLine.Option(names = {"-i", "--interactive"},
            description = "If enabled, will produce one event and wait for <Return>")
    protected boolean interactive;

    @CommandLine.Option(names = {"-c", "--config-file"},
            description = "If provided, content will be added to the properties")
    protected String configFile = null;

    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    protected boolean verbose = false;

    @CommandLine.Option(names = {"--enable-monitoring-interceptor"},
            description = "Enable MonitoringInterceptors (for Control Center)")
    protected boolean monitoringInterceptors = false;

    protected boolean doProduce = true;
    protected int produced = 0;
    protected Random random = new Random();

    private KafkaProducer<Integer, Object> createProducer() {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY);

        addProducerProperties(properties);

        readConfigFile(properties, configFile);

        if (bootstrapServers != null) {
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        if (schemaRegistryURL != null) {
            properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        }

        if (monitoringInterceptors) {
            properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
            properties.put("confluent.monitoring.interceptor.bootstrap.servers", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            properties.put("confluent.monitoring.interceptor.timeout.ms", 3000);
            properties.put("confluent.monitoring.interceptor.publishMs", 10000);
        }

        return new KafkaProducer<>(properties);
    }

    protected final void produce() throws IOException {
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
        }
        else {
            for (int i = 0; i < maxObjects; i++) {
                doProduce(producer);
                if (interactive) {
                    System.out.println("Press any key to continue ...");

                    var c = reader.read();
                }
                if (!doProduce)
                    break;
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

    // Need to override these in the concrete Producer

    protected abstract void addProducerProperties(Properties properties);

    protected abstract ProducerRecord<Integer, Object> createRecord();

}
