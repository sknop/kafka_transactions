package producer;

import common.RegionCode;
import common.TimestampProvider;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;
import schema.Region;

import java.util.Properties;

@CommandLine.Command(name = "RegionProducer",
        version = "RegionProducer 1.0",
        description = "Creates regions and updates the timestamps")
public class RegionProducer extends AbstractProducer {
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
    protected ProducerRecord<Object, Object> createRecord() {
        var nextId = random.nextInt(RegionCode.REGION_CODES.length);

        RegionCode regionCode = RegionCode.getRegion(nextId);

        var code = regionCode.identifier();
        var longName = regionCode.longName();
        var areaCode = regionCode.areaCode();

        String date = TimestampProvider.currentTimestamp();

        Region newRegion = new Region(code, longName, areaCode, date);
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return new ProducerRecord<>(regionTopic, code, newRegion);
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
