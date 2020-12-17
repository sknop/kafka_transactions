package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import schema.Customer;
import schema.OrderItem;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
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
    protected void addProducerProperties(Properties properties) {
    }

    @Override
    protected ProducerRecord<Integer,Object> createRecord() {
        int productId = random.nextInt(100);
        int orderId = random.nextInt(100000);
        int count = random.nextInt(10) + 1;
        float price = (float) (Math.round(random.nextDouble() * 100.0 * 100.0) / 100.0);

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

