# Kafka-reactive-consumer

## Pre-requisites
### Zookeeper running on localhost (port 2181)
### Kafka broker is running on localhost (port 9092)

## Running the Consumer
### Use below command to run the reactive consumer from Command Line. By default, the application will consume dummy messages from "sample-topic"
```
./mvnw spring-boot:run
```

### If you want the application to consume OMS messages from "oms-topic" topic, use this command.
```
./mvnw spring-boot:run -Dspring-boot.run.arguments="--process.sample.topic=false"
```