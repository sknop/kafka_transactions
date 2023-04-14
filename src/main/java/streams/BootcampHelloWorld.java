package streams;

import common.SerdeGenerator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import schema.Customer;

import java.io.*;
import java.util.Properties;

public class BootcampHelloWorld {

    static String inputTopic = "customer";

    static public void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Need property file");
            System.exit(1);
        }
        String propertyFile = args[0];
        Properties properties = new Properties();

        try (InputStream inputStream = new FileInputStream(propertyFile)) {
            Reader reader = new InputStreamReader(inputStream);

            properties.load(reader);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        var consumed = Consumed.with(Serdes.Integer(), SerdeGenerator.<Customer>getSerde(properties));

        // Here the real work starts

        StreamsBuilder builder = new StreamsBuilder();

        KStream<Integer,Customer> inputStream = builder.stream(inputTopic, consumed);

        inputStream
                .filter((key, value) -> value.getAge() < 30)
                .foreach((key, value) ->
                        System.out.println("Key : " + key + " Name = " + value.getLastName() + " Age = " + value.getAge()));

        var stream = new KafkaStreams(builder.build(), properties);

        stream.start();
    }
}
