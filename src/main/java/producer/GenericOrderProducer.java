package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

@CommandLine.Command(name = "GenericOrderProducer",
        mixinStandardHelpOptions = true,
        version = "GenericOrderProducer 1.0",
        description = "Produces Producer objects in Avro format, using the GenericRecord, either a fixed amount or continuously.",
        sortOptions = false)
public class GenericOrderProducer extends AbstractProducer implements Callable<Integer> {
    private static final String DEFAULT_TOPIC = "order";

    @CommandLine.Option(names = {"--topic"},
            description = "Topic for the object (default = ${DEFAULT-VALUE})",
            defaultValue = DEFAULT_TOPIC)
    private String topic;

    @CommandLine.Option(names = {"--avro-schema-file"},
            description = "Avro schema file for the order schema",
            required = true)
    private String avroSchemaFile;

    private Schema schema;

    public GenericOrderProducer() {
    }

    @Override
    protected ProducerRecord<Integer, Object> createRecord() {
        GenericRecord record = new GenericData.Record(schema);

        int orderId = random.nextInt(largestId) + 1;
        int customerId = random.nextInt(largestId) + 1;
        int orderAmount = random.nextInt(largestId) + 1;

        Long date = System.currentTimeMillis();

        record.put("orderId", orderId);
        record.put("date", date);
        record.put("orderAmount", customerId);
        record.put("customerId", Integer.toString(customerId));

        return new ProducerRecord<>(topic, orderId, record);
    }

    private void importSchema() {
        var contentBuilder = new StringBuilder();

        try (Stream<String> stream = Files.lines(Paths.get(avroSchemaFile), StandardCharsets.UTF_8)) {
            stream.forEach((s -> contentBuilder.append(s).append("\n")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.err.println("Inputfile " + avroSchemaFile + " not found");
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Schema.Parser parser = new Schema.Parser();
        schema = parser.parse(contentBuilder.toString());
    }

    @Override
    public Integer call() throws Exception {
        importSchema();
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new GenericOrderProducer()).execute(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
