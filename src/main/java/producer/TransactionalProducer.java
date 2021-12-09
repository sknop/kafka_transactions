package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import schema.Change;
import schema.Revision;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "TransactionalProducer",
        version = "TransactionalProducer 1.0",
        description = "Produces Change and Revision events in Avro format, either a fixed amount or continuously.")
public class TransactionalProducer extends AbstractProducer implements Callable<Integer>  {

    private final static String CHANGE_TOPIC = "change";
    public static final String REVISION_TOPIC = "revision";
    public static final String CHANGE_TRANSACTION_ID = "change-transaction-id";

    @CommandLine.Option(names = {"--change-topic"},
            description = "Topic for change (default = ${DEFAULT-VALUE})",
            defaultValue = CHANGE_TOPIC)
    private String changeTopic;

    @CommandLine.Option(names = {"--revision-topic"},
            description = "Topic for revision (default = ${DEFAULT-VALUE})",
            defaultValue = REVISION_TOPIC)
    private String revisionTopic;

    @CommandLine.Option(names = {"--number-of-changes"},
            description = "Number of total changes",
            required = true)
    private int numberOfChanges;

    @CommandLine.Option(names = {"--number-of-revisions"},
            description = "Total number of revisions per change (default = ${DEFAULT-VALUE})",
            defaultValue = "10")
    private int maxNumberOfFiles;

    @CommandLine.Option(names = {"--change-offset"},
            description = "Start number for changes (default = ${DEFAULT-VALUE})",
            defaultValue = "1")
    private int changeOffset;

    @CommandLine.Option(names = {"--transaction-id"},
            description = "Name of the transaction id (default = ${DEFAULT-VALUE})",
            defaultValue = CHANGE_TRANSACTION_ID)
    private String transactionId;

    final private Random random = new Random();

    public TransactionalProducer() {
    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId);
        super.addProperties(properties);
    }

    @Override
    protected ProducerRecord<Object, Object> createRecord() {
        return null;
    }

    @Override
    protected void produceLoop(KafkaProducer<Object, Object> producer) {
        producer.initTransactions();

        for (int i = 0; i < numberOfChanges; i++) {
            producer.beginTransaction();

            Change change = new Change();
            change.setChange(changeOffset + i);
            change.setDate(Instant.now().getEpochSecond());
            change.setUser("sknop");
            change.setClient("sknop_milo");

            ProducerRecord<Object, Object> changeRecord = new ProducerRecord<>(changeTopic, change.getChange(), change);
            producer.send(changeRecord);

            int numberOfFiles = 1 + random.nextInt(maxNumberOfFiles);
            for (int j = 0; j < numberOfFiles; j++) {
                Revision file = new Revision();
                file.setChange(change.getChange());
                file.setFilename("//depot/project/file" + j);
                file.setRevision(1);
                file.setAction("add");

                ProducerRecord<Object, Object> revisionRecord = new ProducerRecord<>(revisionTopic, change.getChange(), file);
                producer.send(revisionRecord);
            }
            producer.commitTransaction();
        }
    }

    public static void main(String[] args) {
        try {
            new CommandLine(new TransactionalProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
