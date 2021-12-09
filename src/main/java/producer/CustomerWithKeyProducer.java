package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import schema.Customer;
import schema.CustomerId;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "CustomerWithKeyProducer",
        version = "CustomerWithKeyProducer 1.0",
        description = "Produces Customer objects in Avro format with an Avro Key, either a fixed amount or continuously.")
public class CustomerWithKeyProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the customer (default = ${DEFAULT-VALUE})",
            defaultValue = "customer")
    private String customerTopic;

    public CustomerWithKeyProducer() {  }

    public CustomerWithKeyProducer(String bootstrapServers, String schemaRegistries, int maxObjects, String customerTopic) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryURL = schemaRegistries;
        this.maxObjects = maxObjects;
        this.customerTopic = customerTopic;
    }

    @Override
    protected ProducerRecord<Object,Object> createRecord() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss xxx");
        ZonedDateTime now = ZonedDateTime.now();

        String date = dtf.format(now);

        int id = random.nextInt(largestId) + 1;

        String firstName = "first_" + id;
        String lastName = "last_" + id;
        String email = "email_" + id + "@email.com";

        int epoch = 1;

        CustomerId customerId = new CustomerId(id);
        Customer customer = new Customer(id, firstName, lastName, email, date, epoch);

        return new ProducerRecord<>(customerTopic, customerId, customer);
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new CustomerWithKeyProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

