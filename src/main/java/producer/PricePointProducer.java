package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import picocli.CommandLine;
import schema.PricePoint;
import schema.Region;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

@CommandLine.Command(name = "PricePointProducer",
        version = "PricePointProducer 1.0",
        description = "Produces PricePoint objects in Avro format, either a fixed amount or continuously.")
public class PricePointProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--pricepoint-topic"},
            description = "Topic for the price point (default = ${DEFAULT-VALUE})",
            defaultValue = "pricepoint")
    private String pricepointTopic;

    public PricePointProducer() {  }

    @Override
    protected ProducerRecord<Object, Object> createRecord() {
        int productId = random.nextInt(largestId) + 1;
        long date = System.currentTimeMillis();
        float price = (float) ThreadLocalRandom.current().nextDouble(0.0,100.0);

        PricePoint pricePoint = new PricePoint(productId, price, date);

        return new ProducerRecord<>(pricepointTopic, productId, pricePoint);
    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        super.addProperties(properties);
    }

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new PricePointProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

