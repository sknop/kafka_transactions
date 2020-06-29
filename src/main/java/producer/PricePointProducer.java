package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import schema.Customer;
import schema.PricePoint;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
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
    protected void addProducerProperties() {
    }

    @Override
    protected ProducerRecord<Integer,Object> createRecord() {
        int productId = random.nextInt(largestId) + 1;
        long date = System.currentTimeMillis();
        float price = (float) ThreadLocalRandom.current().nextDouble(0.0,100.0);

        PricePoint pricePoint = new PricePoint(productId, price, date);

        return new ProducerRecord<>(pricepointTopic, productId, pricePoint);
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

