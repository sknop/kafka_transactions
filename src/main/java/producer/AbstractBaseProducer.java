package producer;

import common.AbstractBase;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;

public abstract class AbstractBaseProducer<KeyType,ValueType> extends AbstractBase implements Callable<Integer> {

    @CommandLine.Option(names = {"--bootstrap-servers"})
    protected String bootstrapServers;
    @CommandLine.Option(names = {"--schema-registry"})
    protected String schemaRegistryURL;
    @CommandLine.Option(names = {"--enable-monitoring-interceptor"},
            description = "Enable MonitoringInterceptors (for Control Center)")
    protected boolean monitoringInterceptors = false;
    protected boolean doProduce = true;

    @Override
    protected void createProperties() {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, DEFAULT_SCHEMA_REGISTRY);

        addProducerProperties(properties);

        readConfigFile(properties);

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
    }

    protected KafkaProducer<KeyType, ValueType> createProducer() {
        System.out.println("Using properties " + properties);
        return new KafkaProducer<KeyType,ValueType>(properties);
    }

    protected void addProducerProperties(Properties properties) {
        // empty
    }

    protected final void produce() throws IOException {
        createProperties();

        KafkaProducer<KeyType, ValueType> producer = createProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            doProduce = false;
            producer.close();
        }));

        produceLoop(producer);
    }

    protected abstract void produceLoop(KafkaProducer<KeyType, ValueType> producer) throws IOException;

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
    }

}
