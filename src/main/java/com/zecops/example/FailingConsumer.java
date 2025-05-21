package com.zecops.example;

import com.zecops.example.dto.FailingMessage;
import com.zecops.example.dto.Greeting;
import com.zecops.example.dto.Upload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
public class FailingConsumer {

    private ConcurrentHashMap<String, Integer> expectedFailCount = new ConcurrentHashMap<>();

    @Transactional
    @KafkaListener(topics = "failing-messages", groupId = "failing-consumer", containerFactory = "kafkaListenerContainerFactoryForUploads", clientIdPrefix = "${spring.kafka.client-id}-failing-consumer")
    public void processUploads(@Payload FailingMessage failingMessage,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               @Header(KafkaHeaders.RECEIVED_KEY) String key,
                               @Header(KafkaHeaders.OFFSET) int offset) {
        log.info("Received Upload key: " + key);
        Integer newVal = expectedFailCount.compute(key, (k, v) -> {
            if (v == null || v == -1) {
                log.info("The total requested fail count is set to " + failingMessage.getFailCount());
                return failingMessage.getFailCount()-1;
            }
            else {
                if (v > 0)
                    log.info("This message will fail now and then " + (v-1) + " more times.");
                return v - 1;
            }
        });
        if (newVal < 0) {
            log.info("Successfully processed Upload key: " + key);
        } else {
            log.info("Simulating error for key: " + key);
            throw new RuntimeException("Simulated failure of Upload in FailingConsumer for key: " + key);
        }
    }
}
