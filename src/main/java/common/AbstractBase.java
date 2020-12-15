package common;

import java.io.*;
import java.util.Properties;

abstract public class AbstractBase {
    protected static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    protected static final String DEFAULT_SCHEMA_REGISTRY =  "http://localhost:8081";

    protected Properties properties = new Properties();

    public static void readConfigFile(Properties properties, String configFile) {
        if (configFile != null) {
            try (InputStream inputStream = new FileInputStream(configFile)) {
                Reader reader = new InputStreamReader(inputStream);

                properties.load(reader);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.err.println("Inputfile " + configFile + " not found");
                System.exit(1);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    abstract protected void createProperties();
}
