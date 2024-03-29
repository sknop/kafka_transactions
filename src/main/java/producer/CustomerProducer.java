package producer;

import common.RegionCode;
import common.TimestampProvider;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import picocli.CommandLine;
import schema.Customer;

import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "CustomerProducer",
        version = "CustomerProducer 1.1",
        description = "Produces Customer objects in Avro format, either a fixed amount or continuously.")
public class CustomerProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the customer (default = ${DEFAULT-VALUE})",
            defaultValue = "customer")
    private String customerTopic;

    public CustomerProducer() {  }

    public CustomerProducer(String bootstrapServers, String schemaRegistries, int maxObjects, String customerTopic) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryURL = schemaRegistries;
        this.maxObjects = maxObjects;
        this.customerTopic = customerTopic;
    }

    @Override
    protected void addProperties(Properties properties) {
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        super.addProperties(properties);
    }

    @Override
    protected ProducerRecord<Object, Object> createRecord() {
        String date = TimestampProvider.currentTimestamp();

        int customerId = random.nextInt(largestId) + 1;

        String firstName = "first_" + customerId;
        String lastName = "last_" + customerId;
        String email = "email_" + customerId + "@email.com";
        var age = 18 + customerId % 50;

        int epoch = 1;
        var region = RegionCode.getRegion(customerId).identifier();

        Customer customer = new Customer(customerId, firstName, lastName, email, date, age, region, epoch);

        return new ProducerRecord<>(customerTopic, customerId, customer);
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new CustomerProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}

