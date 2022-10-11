package common;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class SerdeGenerator {
    public static <T extends SpecificRecord> SpecificAvroSerde<T> getSerde(Properties context) {
        Map<String,String> map = context.entrySet().stream().collect(
                Collectors.toMap(e -> e.getKey().toString(),
                                 e -> e.getValue().toString())
        );

        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(map, false);
        return serde;
    }
}
