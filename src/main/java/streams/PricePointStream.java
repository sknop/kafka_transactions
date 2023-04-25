package streams;

import common.SerdeGenerator;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import picocli.CommandLine;
import schema.PricePoint;

import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "PricePointStream",
        version = "PricePointStream 1.0",
        description = "Reads PricePoints and prints them.")
public class PricePointStream extends AbstreamStream implements Callable<Integer> {
    final static String PRICEPOINT_TOPIC = "pricepoint";
    final static String BOOTSTRAP_SERVERS = "localhost:9092";
    final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})")
    private String topic = PRICEPOINT_TOPIC;

    public PricePointStream() {
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
    }

    @Override
    protected String getApplicationName() {
        return "price-point-stream";
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        KStream<Integer, PricePoint> pricePoints = builder.stream(topic, Consumed.with(Serdes.Integer(), SerdeGenerator.<PricePoint>getSerde(properties)));

        if (verbose)
            pricePoints.foreach((key, value) -> System.out.println(key + " => " + value));

    }

    public static void main(String[] args) {
        try {
            new CommandLine(new PricePointStream()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}