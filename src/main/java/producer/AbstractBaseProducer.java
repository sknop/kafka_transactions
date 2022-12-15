package producer;

import common.AbstractBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public abstract class AbstractBaseProducer<KeyType,ValueType> extends AbstractBase implements Callable<Integer> {
    protected Logger logger = Logger.getLogger(AbstractBaseProducer.class.getName());

    protected boolean doProduce = true;
    protected Random random = new Random();

    @CommandLine.Option(names = {"-v", "--verbose"},
            description = "If enabled, will print out every message created")
    protected boolean verbose = false;

    protected KafkaProducer<KeyType, ValueType> createProducer() {
        logger.info("Using properties " + properties);
        return new KafkaProducer<>(properties);
    }

    protected void addProperties(Properties properties) {
        super.addProperties(properties);
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
