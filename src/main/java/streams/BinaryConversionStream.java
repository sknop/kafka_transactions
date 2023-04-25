package streams;

import common.SerdeGenerator;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import picocli.CommandLine;
import schema.Binary;
import schema.HexString;

import java.nio.ByteBuffer;
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
    }

    @Override
    protected void createTopology(StreamsBuilder builder) {
        createProperties();

        KStream<Integer, Binary> existingBinary = builder.stream(binaryTopic, Consumed.with(Serdes.Integer(), SerdeGenerator.getSerde(properties)));

        if (verbose)
            existingBinary.foreach((key, value) -> System.out.println(key + " => " + value));

        existingBinary.map((key, value) -> convertKeyValuefromBinary(value))
                .peek((key, value) -> System.out.println(key + " => " + value))
                .to(hexStringTopic, Produced.with(Serdes.String(), SerdeGenerator.getSerde(properties)));
    }

    private KeyValue<String, HexString> convertKeyValuefromBinary(Binary binary) {
        ByteBuffer id = binary.getId();
        StringBuilder key = new StringBuilder();
        for (byte b : id.array()) {
            key.append(String.format("%02X", b));
        }

        return new KeyValue<>(key.toString(), new HexString(key.toString(), binary.getPayload()));
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