# Spark Streaming Exercise
In this Exercise we will be using a data generator utility and Kafka to stream data.

## Prerequisites
1. Java 8
1. Confluent Platform 5.0+
1. Spark 2.3+

###To run Confluent on windows

After you have it installed execute the following in GitBash (Paths will need to be updated to reflect your environment)

Each Command will be in a new window

1. Start Zookeeper

    `$ cd ...pathToConfluent.../confluent-5.1.2/bin/windows`
  
    `$ ./zookeeper-server-start.bat ../../etc/kafka/zookeeper.properties`

1. Start Kafka Server

    `$ cd ...pathToConfluent.../confluent-5.1.2/bin/windows`

    `$ ./kafka-server-start.bat ../../etc/kafka/server.properties`

1. Start the console producer

    `$ cd ...pathToConfluent.../confluent-5.1.2/bin/windows`

    `$ ./kafka-console-producer.bat --broker-list localhost:9092 --topic testTopic`

1. Start the console consumer

    `$ cd ...pathToConfluent.../confluent-5.1.2/bin/windows`

    `$ ./kafka-console-consumer.bat --topic testTopic --bootstrap-server localhost:9092`