package producer;

import org.apache.kafka.clients.producer.*;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;

import java.io.*;
import java.util.Properties;

public abstract class AbstractProducer extends AbstractBaseProducer<Object, Object>
{
    @CommandLine.Option(names = {"-m", "--max"},
            description = "Max numbers of objects to generate/update (default = ${DEFAULT-VALUE}, keep going)")
    protected int maxObjects = -1;

    @CommandLine.Option(names = {"-i", "--interactive"},
            description = "If enabled, will produce one event and wait for <Return>")
    protected boolean interactive;

    @CommandLine.Option(names = {"-l", "--largest"},
            description = "Highest object ID to generate/update (default = ${DEFAULT-VALUE})")
    protected int largestId = 1000;

    @CommandLine.Option(names = {"--delay"},
            description = "Time delay between producing each event (in ms)")
    protected long delay = 0;

    protected int produced = 0;

    public AbstractProducer() {
    }

    @Override
    protected void addProperties(Properties properties) {
        super.addProperties(properties);
    }

    @Override
    protected void produceLoop(KafkaProducer<Object, Object> producer) throws IOException {
        try (var terminal = TerminalBuilder.terminal()) {
            terminal.enterRawMode();
            var reader = terminal.reader();

            int c = 0;
            if (maxObjects == -1) {
                while (doProduce && c != 'q') {
                    singleProduce(producer);

                    if (interactive) {
                        System.out.println("Press any key to continue ...(or 'q' to quit)");

                        c = reader.read();
                    }
                }
            } else {
                for (int i = 0; i < maxObjects; i++) {
                    singleProduce(producer);
                    if (interactive) {
                        System.out.println("Press any key to continue ... (or 'q' to quit)");

                        c = reader.read();
                    }
                    if (!doProduce || c == 'q')
                        break;
                }
            }
        }
        System.out.println("Total produced = " + produced);
    }

    private void singleProduce(KafkaProducer<Object, Object> producer) {
        ProducerRecord<Object, Object> record = createRecord();

        if (verbose) {
            producer.send(record, (recordMetadata, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
                else {
                    var valueSize = recordMetadata.serializedValueSize();
                    System.out.println("Produced ["
                            + valueSize + "] at offset "
                            + recordMetadata.offset() + " with data " + record);
                }
            });
        }
        else {
            producer.send(record); // ignore the record
        }

        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        produced++;
    }

    protected abstract ProducerRecord<Object, Object> createRecord();
}
