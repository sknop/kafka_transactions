package streams;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import picocli.CommandLine;
import schema.Binary;
import schema.HexString;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "BinaryConversionStream",
        version = "BinaryConversionStream 1.0",
        description = "Reads Binary objects in Avro format and converts binary fields to Hex.")
public class BinaryConversionStream extends AbstreamStream implements Callable<Integer> {
    final static String BINARY_TOPIC = "schema.Binary";
    final static String HEXSTRING_TOPIC = "schema.HexString";

    @CommandLine.Option(names = {"--binary-topic"},
            description = "Topic for the Binary object (default = ${DEFAULT-VALUE})")
    private String binaryTopic = BINARY_TOPIC;

    @CommandLine.Option(names = {"--hexstring-topic"},
            description = "Topic for the HexString object (default = ${DEFAULT-VALUE})")
    private String hexStringTopic = HEXSTRING_TOPIC;

    public BinaryConversionStream() {
    }

    @Override
    protected String getApplicationName() {
        return "binary-conversion-stream";
    }

    @Override
    protected void addConsumerProperties(Properties properties) {
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    }

    private void consume() {
        var specificAvroSerde = new SpecificAvroSerde<HexString>();

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://localhost:8081");

        specificAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer, Binary> existingBinary = builder.stream(binaryTopic);

        if (verbose)
            existingBinary.foreach((key, value) -> System.out.println(key + " => " + value));

        existingBinary.map((key, value) -> convertKeyValuefromBinary(value))
                .peek((key, value) -> System.out.println(key + " => " + value))
                .to(hexStringTopic, Produced.with(Serdes.String(), specificAvroSerde));

        KafkaStreams streams = createStreams(builder.build());
        streams.setStateListener((newState, oldState) -> System.out.println("*** Changed state from " +oldState + " to " + newState));
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private HexString convertFromBinary(Binary binary) {
        ByteBuffer id = binary.getId();
        BigInteger value = new BigInteger(id.array());

        return new HexString(value.toString(16), binary.getPayload());
    }

    private KeyValue<String, HexString> convertKeyValuefromBinary(Binary binary) {
        ByteBuffer id = binary.getId();
        BigInteger value = new BigInteger(id.array());
        String key = value.toString(16);

        return new KeyValue<>(key, new HexString(key, binary.getPayload()));
    }

    @Override
    public Integer call() {
        consume();

        return 0;
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new BinaryConversionStream()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}