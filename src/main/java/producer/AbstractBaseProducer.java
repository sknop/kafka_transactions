package producer;

import common.AbstractBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public abstract class AbstractBaseProducer<KeyType,ValueType> extends AbstractBase implements Callable<Integer> {
    protected Logger logger = Logger.getLogger(AbstractBaseProducer.class.getName());

    protected boolean doProduce = true;

    protected KafkaProducer<KeyType, ValueType> createProducer() {
        logger.info("Using properties " + properties);
        return new KafkaProducer<>(properties);
    }

    protected void addProperties(Properties properties) {
        super.addProperties(properties);

        if (monitoringInterceptors) {
            properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
            properties.put("confluent.monitoring.interceptor.bootstrap.servers", properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            properties.put("confluent.monitoring.interceptor.timeout.ms", 3000);
            properties.put("confluent.monitoring.interceptor.publishMs", 10000);
        }
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
