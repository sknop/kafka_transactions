package producer;

import org.apache.kafka.clients.producer.ProducerRecord;
import picocli.CommandLine;
import schema.SensorData;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "SensorDataProducer",
        version = "SensorDataProducer 1.0",
        description = "Produces SensorData objects in Avro format, either a fixed amount or continuously.")
public class SensorDataProducer extends AbstractProducer implements Callable<Integer> {
    @CommandLine.Option(names = {"--sensor-topic"},
            description = "Topic for the customer (default = ${DEFAULT-VALUE})",
            defaultValue = "sensordata")
    private String sensorDataTopic;

    public SensorDataProducer() {  }

    @Override
    protected ProducerRecord<Object,Object> createRecord() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss xxx");
        ZonedDateTime now = ZonedDateTime.now();

        String date = dtf.format(now);

        int sensorId = random.nextInt(largestId) + 1;

        Object value;
        switch (random.nextInt(4)) {
            case 0:
                value = random.nextFloat();
                break;
            case 1:
                value = random.nextInt(100);
                break;
            case 2:
                value = random.nextBoolean();
                break;
            case 3:
                var answer = random.nextBoolean();
                if (answer) {
                    value = "true";
                }
                else {
                    value = "false";
                }
                break;
            default:
                throw new RuntimeException("Should never happen");
        }
        SensorData sensorData = new SensorData(sensorId, date, value);

        return new ProducerRecord<>(sensorDataTopic, sensorId, sensorData);
    }

    @Override
    public Integer call() throws Exception {
        produce();

        return 0;
    }

    public static void main(String[] args) {

        try {
            new CommandLine(new SensorDataProducer()).execute(args);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}

