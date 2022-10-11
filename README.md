# kafka_transactions
Kafka streams and transaction tests

Usage

Create a topic for testing, by default called 'customer'.
Build (IntelliJ + Maven): mvn clean package

This generates a full-fat package named target/kafka_transactions-2.0-SNAPSHOT-jar-with-dependencies.jar

Run the consumer: java -cp target/kafka_transactions-2.0-SNAPSHOT-jar-with-dependencies.jar streams.CustomerStream
Run the one-step producer: java -cp target/kafka_transactions-2.0-SNAPSHOT-jar-with-dependencies.jar producer.CustomerProducer

Press return in the producer to create a new message, observe immediate response in consumer (prints out the message)


Usage:

----

CustomerStream [-hvV] [--enable-monitoring-interceptor]
[--bootstrap-servers=<bootstrapServers>] [-c=<configFile>]
[--scale=<scale>] [--schema-registry=<schemaRegistryURL>]
[--topic=<customerTopic>]

Description:

Reads Customer objects in Avro format from a stream.

Options:

-c, --config-file=<configFile>
If provided, content will be added to the properties
--bootstrap-servers=<bootstrapServers>

      --schema-registry=<schemaRegistryURL>

      --enable-monitoring-interceptor
                        Enable MonitoringInterceptors (for Control Center)
-v, --verbose         If enabled, will print out every message created
--scale=<scale>   If greater than 1, Stream app will increase threads to
the number provided
--topic=<customerTopic>
Topic for the object (default = customer)
-h, --help            Show this help message and exit.
-V, --version         Print version information and exit.

----

CustomerProducer [-hivV] [--enable-monitoring-interceptor]
[--bootstrap-servers=<bootstrapServers>] [-c=<configFile>]
[--customer-topic=<customerTopic>] [-l=<largestId>]
[-m=<maxObjects>] [--schema-registry=<schemaRegistryURL>]

Description:

Produces Customer objects in Avro format, either a fixed amount or continuously.

Options:

-c, --config-file=<configFile>
If provided, content will be added to the properties
--bootstrap-servers=<bootstrapServers>

      --schema-registry=<schemaRegistryURL>

      --enable-monitoring-interceptor
                           Enable MonitoringInterceptors (for Control Center)
-m, --max=<maxObjects>   Max numbers of objects to generate/update (default =
-1, keep going)
-i, --interactive        If enabled, will produce one event and wait for
<Return>
-v, --verbose            If enabled, will print out every message created
-l, --largest=<largestId>
Highest object ID to generate/update (default = 1000)
--customer-topic=<customerTopic>
Topic for the customer (default = customer)
-h, --help               Show this help message and exit.
-V, --version            Print version information and exit.

----

Producer Region (producer.RegionProducer) for reference by customer object

----

New Stream CustomerJoinRegionStream


Requires target topic with the same number of partitions as the region (does that make sense)
Requires lots of ACL permissions to run:

cluster DESCRIBE and IDEMPOTENT_WRITE
topic customer* READ, DESCRIBE, WRITE, CREATE
topic region READ, WRITE, DESCRIBE
