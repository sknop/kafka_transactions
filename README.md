# kafka_transactions
Kafka streams and transaction tests

Usage

Create a topic for testing, by default called 'customer'.
Build (IntelliJ + Maven): mvn clean package

This generates a full-fat package named target/kafka_transactions-1.0-SNAPSHOT-jar-with-dependencies.jar

Run the consumer: java -cp target/kafka_transactions-1.0-SNAPSHOT-jar-with-dependencies.jar streams.CustomerStream
Run the one-step producer: java -cp target/kafka_transactions-1.0-SNAPSHOT-jar-with-dependencies.jar producer.CustomerProducer

Press return in the producer to create a new message, observe immediate response in consumer (prints out the message)

Now change maven file pom.xml to use CP 5.0 and Kafka 2.0. Rebuild project and restart consumer and producer. 
Observe the delay in the response on the consumer - it takes several produced messages before the consumer starts picking up.
