package com.example.kafka.reactive.consumer;

import com.sun.management.OperatingSystemMXBean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.KafkaReceiver;

import java.lang.management.ManagementFactory;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class ReceiverService {
    @Value(value = "${dummy.topic.name}")
    private String sampleTopic;

    @Value(value = "${oms.topic.name}")
    private String omsTopic;

    @Value("${process.sample.topic}")
    private boolean processSampleTopic;

    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");

    private static final AtomicInteger messagesConsumed = new AtomicInteger();

    private final KafkaReceiver<Integer, String> kafkaReceiver;

    private final KafkaReceiver<Integer, Object> omsKafkaReceiver;

    public ReceiverService(KafkaReceiver<Integer, String> receiver, KafkaReceiver<Integer, Object> omsReceiver) {
        this.kafkaReceiver = receiver;
        this.omsKafkaReceiver = omsReceiver;
    }

    public void startReceiving() {

        if (processSampleTopic) {
            log.info("Consuming messages from sample-topic");
            kafkaReceiver.receive().subscribe(r -> {
                messagesConsumed.incrementAndGet();
                r.receiverOffset().acknowledge();
            });
        } else {
            log.info("Consuming messages from oms-topic");
            omsKafkaReceiver.receive().subscribe(r -> {
                messagesConsumed.incrementAndGet();
                r.receiverOffset().acknowledge();
            });
        }
    }

    @Scheduled(initialDelay = 15000, fixedDelay = 15000)
    public void printStats() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
                OperatingSystemMXBean.class);
        // % CPU load this current JVM is taking, from 0.0-1.0
        double processCpuLoad = osBean.getProcessCpuLoad() * 100;

        // % CPU load the overall system is at, from 0.0-1.0
        double systemCpuLoad = osBean.getCpuLoad() * 100;
        log.info("Timestamp/ProcessCpuLoad/SystemCpuLoad/messagesConsumed: \n{}, {}, {}, {}",
                DATE_FORMAT.format(new Date()),
                DECIMAL_FORMAT.format(processCpuLoad),
                DECIMAL_FORMAT.format(systemCpuLoad), messagesConsumed.get());
    }
}
