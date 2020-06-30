package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import schema.Customer;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "CustomerProducer",
        version = "CustomerProducer 1.0",
        description = "Produces Customer objects in Avro format, either a fixed amount or continuously.")
public class CustomerProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--customer-topic"},
            description = "Topic for the customer (default = ${DEFAULT-VALUE})",
            defaultValue = "customer")
    private String customerTopic;

    public CustomerProducer() {  }


    @Override
    protected void addProducerProperties(Properties properties) {
    }

    @Override
    protected ProducerRecord<Integer,Object> createRecord() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss xxx");
        ZonedDateTime now = ZonedDateTime.now();

        String date = dtf.format(now);

        int customerId = random.nextInt(largestId) + 1;

        String firstName = "first_" + customerId;
        String lastName = "last_" + customerId;
        String email = "email_" + customerId + "@email.com";

        int epoch = 1;

        Customer customer = new Customer(customerId, firstName, lastName, email, date, epoch);

        return new ProducerRecord<>(customerTopic, customerId, customer);
    }

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
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

