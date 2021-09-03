package consumer;

import common.AbstractBase;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public abstract class AbstractBaseConsumer<KeyType,ValueType> extends AbstractBase implements Callable<Integer> {
    protected Logger logger = Logger.getLogger(AbstractBaseConsumer.class.getName());

    protected boolean doConsume = true;

    protected KafkaConsumer<KeyType,ValueType> createConsumer() {
        logger.info("Using properties " + properties);
        return new KafkaConsumer<>(properties);
    }

    private void consume() throws IOException{
        createProperties();

        KafkaConsumer<KeyType,ValueType> consumer = createConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            doConsume = false;

            consumer.close();
        }));

        consumeLoop(consumer);
    }

    protected abstract void consumeLoop(KafkaConsumer<KeyType,ValueType> consumer) throws IOException;

    @Override
    public Integer call() throws Exception {
        consume();

        return 0;
    }
}
