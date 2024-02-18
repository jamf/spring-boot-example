package com.zecops.example;

import com.zecops.example.dto.Upload;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class UploadsGenerator {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture<?> runningTask;


    public UploadsGenerator(@Autowired() KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PreDestroy
    synchronized void preDestroy() {
        stop();
        scheduledExecutorService.shutdownNow();
    }

    synchronized boolean startGenerator(int rate, int count) {
        if (runningTask != null) {
            log.info("Already running");
            return false;
        }

        AtomicInteger counter = new AtomicInteger(0);
        runningTask = scheduledExecutorService.scheduleAtFixedRate(() -> {
            Upload upload = new Upload(UUID.randomUUID(), counter.incrementAndGet(), false);
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("uploads", upload.getId().toString(), upload);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Sent upload {} serial {} to partition {} offset {}.", upload.getId(), upload.getSerial(), result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                } else {
                    log.info("Unable to send upload {} serial {}. Exception: {}", upload.getId(), upload.getSerial(), ex.getMessage());
                }
            });

            if (upload.getSerial() >= count) {
                stop();
            }
        }, 0, rate, TimeUnit.MILLISECONDS);
        return true;
    }


    synchronized void stop() {
        if (runningTask != null) {
            runningTask.cancel(true);
            runningTask = null;
            log.info("Generator done/stopped");
        }
    }


}
