package consumer;

import common.AbstractBase;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import picocli.CommandLine;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public abstract class AbstractBaseShareConsumer<KeyType,ValueType> extends AbstractBase implements Callable<Integer> {
    @CommandLine.Option(names = {"--max-messages"},
            description = "Read up to this many messages (subject to batch size). Default -1 --> Keep reading")
    private int maxMessages = -1;

    @CommandLine.Option(names = {"-t", "--threads"},
            defaultValue = "1",
            description = "Number of the threads to spawn to run this consumer in parallel (default: ${DEFAULT-VALUE}).")
    private int numberOfThreads;

    protected Logger logger = Logger.getLogger(AbstractBaseShareConsumer.class.getName());

    protected volatile boolean doConsume = true;
    protected int messagesRead = 0;

    protected KafkaShareConsumer<KeyType,ValueType> createConsumer() {
        logger.info("Using properties " + properties);
        return new KafkaShareConsumer<>(properties);
    }



    private void consume() {
        createProperties();

        KafkaShareConsumer<KeyType,ValueType> consumer = createConsumer();

        consumer.subscribe(getTopicsList());

        while (doConsume) {
            messagesRead += consumeBatch(consumer);
            if (maxMessages > 0 && messagesRead > maxMessages) {
                doConsume = false;
                break;
            }
        }

        System.out.printf("Consumer thread %d shutting down%n", Thread.currentThread().threadId());
        consumer.close();
    }

    protected abstract Collection<String> getTopicsList();

    protected abstract int consumeBatch(KafkaShareConsumer<KeyType,ValueType> consumer);

    @Override
    public Integer call() throws Exception {

        try (ExecutorService service = Executors.newFixedThreadPool(numberOfThreads)) {
            for (int i = 0; i < numberOfThreads; i++)
                service.execute(this::consume);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down gracefully ...");
                doConsume = false;

                service.shutdown();
                System.out.println("\n*** Initiated shutdown. Main thread waiting for tasks to complete... ***\n");

                boolean terminated;
                try {
                    terminated = service.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                if (terminated) {
                    System.out.println("Terminated cleanly");
                }
                else {
                    System.out.println("Timed out");
                }

            }));


        }

        return 0;
    }
}
