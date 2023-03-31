# kafka-reactive-consumer

# Pre-requisites
## zookeeper is running on localhost (port 2181)
## kafka broker is running on localhost (port 9092)

# Use below command to run the reactive consumer from Command Line. By default, the application will consume dummy messages from "sample-topic"
./mvnw spring-boot:run

# If you want the application to consume OMS messages from "oms-topic" topic, use this command.
./mvnw spring-boot:run -Dspring-boot.run.arguments="--process.sample.topic=false"