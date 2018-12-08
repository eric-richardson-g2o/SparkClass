#Start Kafka

1. make sure confluent.io is on your path
        
       which confluent
       
2. start the confluent platform

       confluent start

#Test the install

1. start the console consumer

       kafka-console-consumer --bootstrap-server localhost:9092 --topic consoleTopic

2. In a new console window - start the console producer

       kafka-console-producer --broker-list localhost:9092 --topic consoleTopic

2. Type words, check to see that what you type shows up in the consumer console

3. You can stop the consumer, keep the producer open
