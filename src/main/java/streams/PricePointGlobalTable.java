package streams;

import common.SerdeGenerator;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import picocli.CommandLine;
import schema.PricePoint;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "PricePointGlobalTable",
        version = "PricePointGlobalTable 1.0",
        description = "Reads PricePoints and updates a state store.")
public class PricePointGlobalTable extends AbstreamStream {
    final static String PRICEPOINT_TOPIC = "pricepoint";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String topic = PRICEPOINT_TOPIC;

    public PricePointGlobalTable() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    @Override
    protected String getApplicationName() {
        return "price-point-global-table";
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        GlobalKTable<Integer, PricePoint> pricePoints = builder.globalTable(topic,
                Materialized.<Integer, PricePoint, KeyValueStore<Bytes, byte[]>>as("global.price.table")
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(SerdeGenerator.<PricePoint>getSerde(properties))
        );

//        if (verbose)
//            pricePoints.
//                    toStream().
//                    foreach((key, value) -> System.out.println(key + " => " + value));
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new PricePointGlobalTable()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}