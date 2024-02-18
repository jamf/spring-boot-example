package com.zecops.example;

import com.zecops.example.dto.Upload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
public class UploadsProcessor {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public UploadsProcessor(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "uploads", groupId = "upload-processors", containerFactory = "kafkaListenerContainerFactoryForUploads", clientIdPrefix = "uploadsProcessor")
    public void processUploads(@Payload Upload upload,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               @Header(KafkaHeaders.RECEIVED_KEY) String key,
                               @Header(KafkaHeaders.OFFSET) int offset) {
        log.info("Received Upload in partition {}, offset {}, key {} : {}", partition, offset, key, upload);
        Random random = ThreadLocalRandom.current();
        if (random.nextInt(10) == 0) {
            throw new RuntimeException("Simulated failure 1");
        }
        try {
            Thread.sleep(random.nextInt(10000));
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
        if (random.nextInt(10) == 0) {
            throw new RuntimeException("Simulated failure 2");
        }

        upload.setProcessed(true);
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("uploads-processed", upload.getId().toString(), upload);
        try {
            SendResult<String, Object> result = future.get();
            log.info("Sent processed upload {} serial {} to partition {} offset {}.", upload.getId(), upload.getSerial(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } catch (ExecutionException e) {
            log.info("Unable to send processed upload {} serial {}. Exception: {}", upload.getId(), upload.getSerial(), e.getMessage());
            throw new RuntimeException("Send failure", e);
        }
    }




}
