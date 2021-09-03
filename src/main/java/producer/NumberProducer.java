package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Properties;

@CommandLine.Command(name = "NumberProducer",
        version = "NumberProducer 1.0",
        description = "Creates consecutive numbers up to a certain number")
public class NumberProducer extends AbstractBaseProducer<Void, Long> {
    @CommandLine.Option(names = {"--number-topic"},
            description = "Topic for the numbers (default = ${DEFAULT-VALUE})",
            defaultValue = "numbers")
    private String numberTopic = "numbers";

    @CommandLine.Option(names = {"--max-number"},
            description = "Maximum number to produce (default = ${DEFAULT-VALUE})",
            defaultValue = "10000")
    private long maxNumber = 10000;

    @Override
    protected void produceLoop(KafkaProducer<Void, Long> producer) throws IOException {
        for (long number = 1; number < maxNumber; number++) {
            ProducerRecord<Void,Long> record = new ProducerRecord<Void,Long>(numberTopic, number);

            producer.send(record, (recordMetadata, e) -> {
                Logger logger = LoggerFactory.getLogger(NumberProducer.class);
                if (e == null) {
                    logger.info("Successfully received the details as: \n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() + "\n" +
                            "SerializedValueSize: " +recordMetadata.serializedValueSize());
                } else {
                    logger.error("Can't produce,getting error", e);
                }
            });
            if (!doProduce)
                break;
            }
    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, VoidSerializer.class);

        super.addProperties(properties);
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new NumberProducer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
