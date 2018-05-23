package producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.sourceforge.argparse4j.ArgumentParserBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import schema.Change;
import schema.Revision;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

public class TransactionalProducer {

    private final static String CHANGE_TOPIC = "CHANGE";
    public static final String REVISION_TOPIC = "REVISION";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    public static final String CHANGE_TRANSACTION_ID = "change-transaction-id";

    private String changeTopic;
    private String revisionTopic;
    private int numberOfChanges;
    private int maxNumberOfFiles;
    private int changeOffset;
    private String bootstrapServers;
    private String schemaRegistryURL;

    private Random random = new Random();

    public TransactionalProducer(Namespace options) {
        numberOfChanges = options.getInt("changes");
        maxNumberOfFiles = options.getInt("range");
        changeOffset = options.getInt("offset");
        changeTopic = options.get("change_topic");
        revisionTopic = options.get("revision_topic");
        bootstrapServers = options.get("bootstrap_servers");
        schemaRegistryURL = options.get("schema_registry");
    }

    private KafkaProducer<Integer, Object> createProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryURL);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, CHANGE_TRANSACTION_ID);

        KafkaProducer<Integer, Object> producer = new KafkaProducer<>(properties);

        return producer;
    }

    public void runProducer() {
        KafkaProducer<Integer, Object> producer = createProducer();

        producer.initTransactions();

        for (int i = 0; i < numberOfChanges; i++) {
            producer.beginTransaction();

            Change change = new Change();
            change.setChange(changeOffset + i);
            change.setDate(Instant.now().getEpochSecond());
            change.setUser("sknop");
            change.setClient("sknop_milo");

            ProducerRecord<Integer, Object> changeRecord = new ProducerRecord<>(changeTopic, change.getChange(), change);
            producer.send(changeRecord);

            int numberOfFiles = 1 + random.nextInt(maxNumberOfFiles);
            for (int j = 0; j < numberOfFiles; j++) {
                Revision file = new Revision();
                file.setChange(change.getChange());
                file.setFilename("//depot/project/file" + j);
                file.setRevision(1);
                file.setAction("add");

                ProducerRecord<Integer, Object> revisionRecord = new ProducerRecord<>(revisionTopic, change.getChange(), file);
                producer.send(revisionRecord);
            }
            producer.commitTransaction();
        }
        producer.close();
    }

    public static void main(String[] args) {
        ArgumentParserBuilder builder = ArgumentParsers.newFor("TransactionalProducer").addHelp(true);

        ArgumentParser parser = builder.build();
        parser.addArgument("-c", "--changes")
                .required(true)
                .type(Integer.class)
                .help("Number of changes to generate");
        parser.addArgument("-r", "--range")
                .type(Integer.class)
                .setDefault(10)
                .help("Maximum number of changes (default 10)");
        parser.addArgument("-o", "--offset")
                .setDefault(1)
                .type(Integer.class)
                .help("Offset for changes (default: 1");
        parser.addArgument("--change-topic")
                .type(String.class)
                .setDefault(CHANGE_TOPIC)
                .help(String.format("Topic for the change (default %s)",CHANGE_TOPIC));
        parser.addArgument("--revision-topic")
                .type(String.class)
                .setDefault(REVISION_TOPIC)
                .help(String.format("Topic for files (default %s", REVISION_TOPIC));
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

            TransactionalProducer producer = new TransactionalProducer(options);
            producer.runProducer();

        } catch (ArgumentParserException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(parser.formatHelp());
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}
