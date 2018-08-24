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
import schema.Customer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumeAndProduceTwo {
    final static String CUSTOMER_TOPIC = "customer";
    final static String BOOTSTRAP_SERVERS = "localhost:9092";
    final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    final static String TOPIC1 = "topic1";
    final static String TOPIC2 = "topic2";

    private String customerTopic;
    private String bootstrapServers;
    private String schemaRegistryURL;

    private String topic1 = TOPIC1;
    private String topic2 = TOPIC2;

    public ConsumeAndProduceTwo(Namespace options) {
        customerTopic = options.get("customer_topic");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    public void consumeAndProduce() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "consumeAndProduceTwo");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumerProps.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        consumerProps.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerProps.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);

        try(KafkaConsumer<Integer, Customer> consumer = new KafkaConsumer<>(consumerProps);
            KafkaProducer<Integer, Customer> prod1 = new KafkaProducer<>(producerProps);
            KafkaProducer<Integer, Customer> prod2 = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(Arrays.asList(customerTopic));

            Duration duration = Duration.ofMillis(100);

            while(true) {
                // ConsumerRecords<Integer, Customer> records = consumer.poll(duration);
                ConsumerRecords<Integer, Customer> records = consumer.poll(100);
                for (ConsumerRecord<Integer, Customer> record : records) {
                    System.out.println("Found " + record.value());

                    ProducerRecord<Integer, Customer> record1 = new ProducerRecord<>(topic1, record.key(), record.value());
                    prod1.send(record1);

                    ProducerRecord<Integer, Customer> record2 = new ProducerRecord<>(topic2, record.key(), record.value());
                    prod2.send(record2);

                }
                consumer.commitSync();
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("CustomerProducer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("--customer-topic")
                .type(String.class)
                .setDefault(CUSTOMER_TOPIC)
                .help(String.format("Topic for the customer (default %s)", CUSTOMER_TOPIC));
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

            ConsumeAndProduceTwo app = new ConsumeAndProduceTwo(options);
            app.consumeAndProduce();
        } catch (ArgumentParserException e) {
            System.err.println(parser.formatHelp());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
