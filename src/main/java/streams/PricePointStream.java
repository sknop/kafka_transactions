package streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
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
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    @Override
    protected String getApplicationName() {
        return "price-point-stream";
    }

    private void consume() {
        StreamsBuilder builder = new StreamsBuilder();
        var integerSerde = Serdes.Integer();
        var specificSerde = new SpecificAvroSerde<PricePoint>();


        KStream<Integer, PricePoint> pricePoints = builder.stream(topic);

        if (verbose)
            pricePoints.foreach((key, value) -> System.out.println(key + " => " + value));

        KafkaStreams streams = createStreams(builder.build());
        streams.setStateListener((newState, oldState) -> System.out.println("*** Changed state from " +oldState + " to " + newState));
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


    @Override
    public Integer call() {
        consume();

        return 0;
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