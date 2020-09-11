package consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import schema.SensorData;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumeSensorData {
    final static String CUSTOMER_TOPIC = "sensorData";
    final static String BOOTSTRAP_SERVERS = "localhost:9092";
    final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private final String sensorTopic;
    private final String bootstrapServers;
    private final String schemaRegistryURL;

    public ConsumeSensorData(Namespace options) {
        sensorTopic = options.get("sensor_topic");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    public void consumeAndProduce() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumerSensorData");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        try(KafkaConsumer<Integer, SensorData> consumer = new KafkaConsumer<>(consumerProps)) {

            consumer.subscribe(Arrays.asList(sensorTopic));

            Duration duration = Duration.ofMillis(100);

            while(true) {
                ConsumerRecords<Integer, SensorData> records = consumer.poll(duration);
                for (ConsumerRecord<Integer, SensorData> record : records) {
                    System.out.println("Found " + record.value());
                }
                consumer.commitSync();
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("SensorDataConsumer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--sensor-topic")
                .type(String.class)
                .setDefault(CUSTOMER_TOPIC)
                .help(String.format("Topic for the sensorData (default %s)", CUSTOMER_TOPIC));
        parser.addArgument("--bootstrap-servers")
                .type(String.class)
                .setDefault(BOOTSTRAP_SERVERS)
                .help(String.format("Kafka Bootstrap Servers(default %s)", BOOTSTRAP_SERVERS));
        parser.addArgument("--schema-registry")
                .type(String.class)
                .setDefault(SCHEMA_REGISTRY_URL)
                .help(String.format("Schema registry URL(de fault %s)", SCHEMA_REGISTRY_URL));

        try {
            Namespace options = parser.parseArgs(args);

            ConsumeSensorData app = new ConsumeSensorData(options);
            app.consumeAndProduce();
        } catch (ArgumentParserException e) {
            System.err.println(parser.formatHelp());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
