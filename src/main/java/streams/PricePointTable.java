package streams;

import common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import picocli.CommandLine;
import schema.PricePoint;

import java.util.Properties;

@CommandLine.Command(name = "PricePointTable",
        version = "PricePointTable 1.0",
        description = "Reads PricePoints and updates a state store.")
public class PricePointTable extends AbstreamStream {
    final static String PRICEPOINT_TOPIC = "pricepoint";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String topic = PRICEPOINT_TOPIC;

    public PricePointTable() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    @Override
    protected String getApplicationName() {
        return "price-point-table";
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KTable<Integer, PricePoint> pricePoints = builder.table(topic,
                Materialized.<Integer, PricePoint, KeyValueStore<Bytes, byte[]>>as("price.table")
                        .withKeySerde(Serdes.Integer())
                        .withValueSerde(SerdeGenerator.<PricePoint>getSerde(properties))
        );

        if (verbose)
            pricePoints.
                    toStream().
                    foreach((key, value) -> System.out.println(key + " => " + value));
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new PricePointTable()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}