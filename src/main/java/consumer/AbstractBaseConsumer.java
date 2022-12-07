package consumer;

import common.AbstractBase;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public abstract class AbstractBaseConsumer<KeyType,ValueType> extends AbstractBase implements Callable<Integer> {
    @CommandLine.Option(names = {"--max-messages"},
            description = "Read up to this many messages (subject to batch size). Default -1 --> Keep reading")
    private int maxMessages = -1;

    protected Logger logger = Logger.getLogger(AbstractBaseConsumer.class.getName());

    protected boolean doConsume = true;
    protected int messagesRead = 0;

    protected KafkaConsumer<KeyType,ValueType> createConsumer() {
        logger.info("Using properties " + properties);
        return new KafkaConsumer<>(properties);
    }



    private void consume() throws IOException{
        createProperties();

        var consumerBalanceListener = new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("*** Partitions revoked : " + collection.toString());
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("*** Partitions assigned : " + collection.toString());
            }

            @Override
            public void onPartitionsLost(Collection<TopicPartition> partitions) {
                System.out.println("*** Partitions lost : " + partitions.toString());
                ConsumerRebalanceListener.super.onPartitionsLost(partitions);
            }
        };

        KafkaConsumer<KeyType,ValueType> consumer = createConsumer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            doConsume = false;
        }));

        consumer.subscribe(getTopicsList(), consumerBalanceListener);

        while (doConsume) {
            messagesRead += consumeBatch(consumer);
            if (maxMessages > 0 && messagesRead > maxMessages) {
                doConsume = false;
                break;
            }
        }

        consumer.close();
    }

    protected abstract Collection<String> getTopicsList();

    protected abstract void subscribe(KafkaConsumer<KeyType, ValueType> consumer);
    protected abstract int consumeBatch(KafkaConsumer<KeyType,ValueType> consumer);

    @Override
    public Integer call() throws Exception {
        consume();

        return 0;
    }
}
