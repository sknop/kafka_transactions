package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import picocli.CommandLine;
import schema.Binary;
import schema.Region;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "BinaryProducer",
        version = "BinaryProducer 1.0",
        description = "Produces Binary objects in Avro format, either a fixed amount or continuously.")
public class BinaryProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--binary-topic"},
            description = "Topic for the customer (default = ${DEFAULT-VALUE})")
    private String binaryTopic = "binary";
    private BigInteger counter = new BigInteger("430E009A80B53D7896A57BFE4DD91954", 16);

    public BinaryProducer() {  }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        super.addProperties(properties);
    }

    @Override
    protected ProducerRecord<Object, Object> createRecord() {

        counter = counter.add(BigInteger.valueOf(1));

        var buffer = ByteBuffer.wrap(counter.toByteArray());
        String hexKey = counter.toString(16);

        Binary binary = new Binary(buffer, hexKey);

        return new ProducerRecord<>(binaryTopic, counter.intValue(), binary);
    }

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new BinaryProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

