package com.zecops.example;

import com.zecops.example.dto.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.kafka.support.KafkaHeaders;

import java.util.concurrent.*;

@Slf4j
@Service
public class GreetingsConsumer {

    private final BlockingQueue<Greeting> queue = new LinkedBlockingQueue<>();

    // This is "broadcast" so no group ID specified. The default uses spring.kafka.consumer.group-id=server-${server.port} so it is different cross servers
    @KafkaListener(topics = "greetings")
    public void listenGreetings(@Payload Greeting greeting,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.GROUP_ID) String groupId,
                                @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                @Header(KafkaHeaders.OFFSET) int offset) {
        log.info("Received Message in group '{}' partition {}, offset {}, key {} : {}", groupId, partition, offset, key, greeting);
        queue.add(greeting);
    }

    Greeting getMessageSync() {
        try {
            return queue.poll(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
            return null;
        }
    }
}
