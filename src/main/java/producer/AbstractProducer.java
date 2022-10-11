package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public abstract class AbstractProducer extends AbstractBaseProducer<Object, Object>
{
    @CommandLine.Option(names = {"-m", "--max"},
            description = "Max numbers of objects to generate/update (default = ${DEFAULT-VALUE}, keep going)")
    protected int maxObjects = -1;

    @CommandLine.Option(names = {"-i", "--interactive"},
            description = "If enabled, will produce one event and wait for <Return>")
    protected boolean interactive;

    @CommandLine.Option(names = {"-l", "--largest"},
            description = "Highest object ID to generate/update (default = ${DEFAULT-VALUE})")
    protected int largestId = 1000;

    protected int produced = 0;

    public AbstractProducer() {
    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        super.addProperties(properties);
    }

    @Override
    protected void produceLoop(KafkaProducer<Object, Object> producer) throws IOException {
        var terminal = TerminalBuilder.terminal();
        terminal.enterRawMode();
        var reader = terminal.reader();

        int c = 0;
        if (maxObjects == -1) {
            while (doProduce && c != 'q') {
                singleProduce(producer);

                if (interactive) {
                    System.out.println("Press any key to continue ...(or 'q' to quit)");

                    c = reader.read();
                }
            }
        }
        else {
            for (int i = 0; i < maxObjects; i++) {
                singleProduce(producer);
                if (interactive) {
                    System.out.println("Press any key to continue ... (or 'q' to quit)");

                    c = reader.read();
                }
                if (!doProduce || c == 'q')
                    break;
            }
        }

        System.out.println("Total produced = " + produced);
    }

    private void singleProduce(KafkaProducer<Object, Object> producer) {
        ProducerRecord<Object, Object> record = createRecord();
        int valueSize = 0;

        Future<RecordMetadata> future = producer.send(record);
        try {
            RecordMetadata result = future.get();
            valueSize = result.serializedValueSize();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        if (verbose)
            System.out.println("Produced [" +valueSize + "] " + record);
        produced++;
    }

    protected abstract ProducerRecord<Object, Object> createRecord();
}
