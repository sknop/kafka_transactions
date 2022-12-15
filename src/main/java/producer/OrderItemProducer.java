package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import picocli.CommandLine;
import schema.OrderItem;
import schema.Region;

import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "OrderItemProducer",
        version = "OrderItemProducer 1.0",
        description = "Produces OrderItem objects in Avro format, either a fixed amount or continuously.")
public class OrderItemProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--orderitem-topic"},
            description = "Topic for the customer (default = ${DEFAULT-VALUE})",
            defaultValue = "schema.OrderItem")
    private String orderItemTopic;

    public OrderItemProducer() {  }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        super.addProperties(properties);
    }

    @Override
    protected ProducerRecord<Object, Object> createRecord() {
        var productId = random.nextInt(100);
        var orderId = random.nextInt(100000);
        var count = random.nextInt(10) + 1;
        var price = (Math.round(random.nextDouble() * 100.0 * 100.0) / 100.0);

        OrderItem orderItem = new OrderItem(productId, orderId, count, price);

        return new ProducerRecord<>(orderItemTopic, orderId, orderItem);
    }

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new OrderItemProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

