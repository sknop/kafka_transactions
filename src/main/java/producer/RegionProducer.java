package producer;

import common.RegionCode;
import common.TimestampProvider;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;
import schema.Region;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@CommandLine.Command(name = "RegionProducer",
        version = "RegionProducer 1.0",
        description = "Creates regions and updates the timestamps")
public class RegionProducer extends AbstractBaseProducer<String, Region> {
    @CommandLine.Option(names = {"--region-topic"},
            description = "Topic for the regions (default = ${DEFAULT-VALUE})",
            defaultValue = "region")
    private String regionTopic;

    @CommandLine.Option(names = {"--delay"},
            description = "Time delay between producing each event (in ms)")
    private long delay = 0;

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        super.addProperties(properties);
    }

    @Override
    protected void produceLoop(KafkaProducer<String, Region> producer) {

        int totalProduced = 0;

        while(doProduce) {
            var nextId = random.nextInt(RegionCode.REGION_CODES.length);

            RegionCode regionCode = RegionCode.getRegion(nextId);

            var code = regionCode.identifier();
            var longName = regionCode.longName();
            var areaCode = regionCode.areaCode();

            String date = TimestampProvider.currentTimestamp();

            Region newRegion = new Region(code, longName, areaCode, date);
            ProducerRecord<String, Region> record = new ProducerRecord<>(regionTopic, code, newRegion);

            if (!doProduce)
                break;

            Future<RecordMetadata> future = producer.send(record);
            try {
                RecordMetadata result = future.get();
                int valueSize = result.serializedValueSize();

                if (verbose)
                    System.out.println("Produced [" +valueSize + "] " + record);

                totalProduced++;

            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            if (delay > 0) {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        System.out.println("Produced a total of " + totalProduced);
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new RegionProducer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
