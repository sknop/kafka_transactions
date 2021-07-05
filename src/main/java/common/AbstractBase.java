package common;

import picocli.CommandLine;

import java.io.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@CommandLine.Command(
        synopsisHeading = "%nUsage:%n",
        descriptionHeading   = "%nDescription:%n%n",
        parameterListHeading = "%nParameters:%n%n",
        optionListHeading    = "%nOptions:%n%n",
        mixinStandardHelpOptions = true,
        sortOptions = false)
abstract public class AbstractBase {
    protected static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    protected static final String DEFAULT_SCHEMA_REGISTRY =  "http://localhost:8081";
    protected Logger logger = Logger.getLogger("AbstractBase");

    protected Properties properties = new Properties();

    @CommandLine.Option(names = {"-c", "--config-file"},
            description = "If provided, content will be added to the properties")
    protected String configFile = null;

    public void readConfigFile(Properties properties) {
        if (configFile != null) {
            logger.info("Reading config file " + configFile);

            try (InputStream inputStream = new FileInputStream(configFile)) {
                Reader reader = new InputStreamReader(inputStream);

                properties.load(reader);
                logger.info(properties.entrySet()
                            .stream()
                            .map(e -> e.getKey() + " : " + e.getValue())
                            .collect(Collectors.joining(", ")));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                logger.severe("Inputfile " + configFile + " not found");
                System.exit(1);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            logger.warning("No config file specified");
        }
    }

    abstract protected void createProperties();
}
