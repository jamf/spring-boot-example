package com.zecops.example;

import com.zecops.example.dto.Upload;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.*;

@Slf4j
@Component
@Profile("verifier")
public class ProcessedUploadsVerifier {

    private final PriorityQueue<Upload> priorityQueue = new PriorityQueue<>(100, Comparator.comparingLong(Upload::getSerial));
    private final List<Upload> duplications = new LinkedList<>();
    private volatile int lastReceivedSerial = 0;
    private volatile boolean unprocessedDetected = false;

    @KafkaListener(topics = "uploads-processed", groupId = "processed-uploads-validator", containerFactory = "kafkaListenerContainerFactoryForUploads", clientIdPrefix = "uploadsVerifier")
    public void processUploads(@Payload Upload upload,
                               @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               @Header(KafkaHeaders.RECEIVED_KEY) String key,
                               @Header(KafkaHeaders.OFFSET) int offset) {
        log.info("Received Processed Upload in partition {}, offset {}, key {} : {}", partition, offset, key, upload);

        if (!upload.isProcessed()) {
            unprocessedDetected = true;
            log.info("Unprocessed upload detected: {}", upload);
        }

        synchronized (this) {
            if (upload.getSerial() == lastReceivedSerial + 1) {
                lastReceivedSerial++;

                Upload fromQueue;
                while ((fromQueue = priorityQueue.peek()) != null) {
                    if (fromQueue.getSerial() == lastReceivedSerial + 1) {
                        lastReceivedSerial++;
                        priorityQueue.poll();
                    } else if (fromQueue.getSerial() > lastReceivedSerial + 1) {
                        break;
                    }
                }
                if (priorityQueue.isEmpty()) {
                    log.info("All aligned up to serial: {}. Unprocessed detected: {}", lastReceivedSerial, unprocessedDetected);
                }
            } else if (upload.getSerial() < lastReceivedSerial + 1) {
                duplications.add(upload);
                log.info("Duplicated: {}", upload.getSerial());
            } else {
                priorityQueue.add(upload);
                log.info("Out of order: {}", upload.getSerial());
            }
        }
    }

    synchronized void reset() {
        priorityQueue.clear();
        lastReceivedSerial = 0;
        unprocessedDetected = false;
        duplications.clear();
    }

    synchronized Status getStatus() {
        return new Status(unprocessedDetected, lastReceivedSerial, priorityQueue.size(), new ArrayList<>(duplications));
    }


    @Data
    public static class Status {
        private final boolean unprocessedDetected;
        private final int lastReceivedSerial;
        private final int outOfOrderQueued;
        private final List<Upload> duplications;
    }
}
