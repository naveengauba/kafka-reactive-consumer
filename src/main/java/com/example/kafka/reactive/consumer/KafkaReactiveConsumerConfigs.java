package com.example.kafka.reactive.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.Properties;

@Configuration
public class KafkaReactiveConsumerConfigs {

    @Value(value = "${kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${consumer.group.id}")
    private String GROUP_ID;

    @Value(value = "${dummy.topic.name}")
    private String sampleTopic;

    @Value(value = "${oms.topic.name}")
    private String omsTopic;

    @Bean
    public KafkaReceiver<Integer, String> dummyKafkaReceiver() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ReceiverOptions<Integer, String> receiverOptions = ReceiverOptions.<Integer, String>create(consumerProps).subscription(Collections.singleton(sampleTopic));
        return KafkaReceiver.create(receiverOptions);
    }

    @Bean
    public KafkaReceiver<Integer, Object> omsKafkaReceiver() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomObjectDeserializer.class.getName());

        ReceiverOptions<Integer, Object> receiverOptions = ReceiverOptions.<Integer, Object>create(consumerProps).subscription(Collections.singleton(omsTopic));
        return KafkaReceiver.create(receiverOptions);
    }

}
