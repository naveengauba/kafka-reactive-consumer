package com.example.kafka.reactive.consumer;

import com.sun.management.OperatingSystemMXBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@EnableScheduling
@Slf4j
public class KafkaReactiveConsumerApplication implements DisposableBean {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    private static final String GROUP_ID = "reactive-consumer";

    public static final String SAMPLE_TOPIC = "sample-topic";

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

    private static KafkaReceiver<Integer, String> kafkaReceiver;

    private static final AtomicInteger messagesConsumed = new AtomicInteger();


    public static void main(String[] args) {
        SpringApplication.run(KafkaReactiveConsumerApplication.class, args);

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        ReceiverOptions<Integer, String> receiverOptions = ReceiverOptions.<Integer, String>create(consumerProps).subscription(Collections.singleton(SAMPLE_TOPIC));
        kafkaReceiver = KafkaReceiver.create(receiverOptions);

        Flux<ReceiverRecord<Integer, String>> inboundFlux = kafkaReceiver.receive();

        inboundFlux.subscribe(r -> {
            messagesConsumed.incrementAndGet();
            //System.out.printf("Received message: %s\n", r);
            r.receiverOffset().acknowledge();
        });
    }


    @Scheduled(initialDelay = 15000, fixedDelay = 15000)
    public void printStats() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        // % CPU load this current JVM is taking, from 0.0-1.0
        double processCpuLoad = osBean.getProcessCpuLoad() * 100;

        // % CPU load the overall system is at, from 0.0-1.0
        double systemCpuLoad = osBean.getCpuLoad() * 100;
        log.info("Timestamp/ProcessCpuLoad/SystemCpuLoad/messagesConsumed: \n{}, {}, {}, {}", DATE_FORMAT.format(new Date()), DECIMAL_FORMAT.format(processCpuLoad), DECIMAL_FORMAT.format(systemCpuLoad), messagesConsumed.get());
    }

    @Override
    public void destroy() {
        log.info("Shutting down KafkaRegularConsumerApplication");
        // kafkaReceiver.close();
    }
}
