package consumer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import schema.Counter;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class CounterConsumer {
    final static String COUNTER_TOPIC = "counter";
    final static String BOOTSTRAP_SERVERS = "localhost:9092";
    final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private String counterTopic;
    private String bootstrapServers;
    private String schemaRegistryURL;

    private boolean doConsume = true;

    public CounterConsumer(Namespace options) {
        counterTopic = options.get("counter_topic");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    public void consumeAndProduce() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumeCounter");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gracefully ...");
            doConsume = false;
        }));

        try(KafkaConsumer<Integer, Counter> consumer = new KafkaConsumer<>(consumerProps)) {

            consumer.subscribe(Arrays.asList(counterTopic));

            Duration duration = Duration.ofMillis(100);

            while(doConsume) {
                ConsumerRecords<Integer, Counter> records = consumer.poll(duration);
                for (ConsumerRecord<Integer, Counter> record : records) {
                    System.out.println("Found " + record.value());

                }
                consumer.commitSync();
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("CustomerProducer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--counter-topic")
                .type(String.class)
                .setDefault(COUNTER_TOPIC)
                .help(String.format("Topic for the customer (default %s)", COUNTER_TOPIC));
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

            CounterConsumer app = new CounterConsumer(options);
            app.consumeAndProduce();
        } catch (ArgumentParserException e) {
            System.err.println(parser.formatHelp());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
