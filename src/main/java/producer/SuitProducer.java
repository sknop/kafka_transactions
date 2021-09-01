package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import schema.Suit;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "SuitProducer",
        version = "SuitProducer 1.0",
        description = "Produces Customer objects in Avro format, either a fixed amount or continuously.")
public class SuitProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--suit-topic"},
            description = "Topic for the Suit (default = ${DEFAULT-VALUE})",
            defaultValue = "suit")
    private String suitTopic;

    public SuitProducer() {  }


    @Override
    protected ProducerRecord<Integer,Object> createRecord() {

        Suit suit = new Suit();
        suit.setSuit(schema.enums.Suit.DIAMONDS);


        return new ProducerRecord<>(suitTopic, 1, suit);
    }

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new SuitProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

