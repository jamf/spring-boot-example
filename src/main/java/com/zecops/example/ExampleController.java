package com.zecops.example;

import com.zecops.example.dto.Greeting;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
public class ExampleController {
    
    private final ConfigurableApplicationContext configurableApplicationContext;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final GreetingsConsumer greetingsConsumer;
    private final UploadsGenerator uploadsGenerator;
    private final ProcessedUploadsVerifier processedUploadsVerifier;

    @Autowired
    public ExampleController(ConfigurableApplicationContext configurableApplicationContext, GreetingsConsumer greetingsConsumer, KafkaTemplate<String, Object> kafkaTemplate,
                             UploadsGenerator uploadsGenerator, @Autowired(required = false) ProcessedUploadsVerifier processedUploadsVerifier) {
        this.configurableApplicationContext = configurableApplicationContext;
        this.greetingsConsumer = greetingsConsumer;
        this.kafkaTemplate = kafkaTemplate;
        this.uploadsGenerator = uploadsGenerator;
        this.processedUploadsVerifier = processedUploadsVerifier;
    }

    @GetMapping()
    public ExampleDto helloWorldFromMemory() {
        log.info("Got request to return Hello world.");
        ExampleDto exampleDto = new ExampleDto();
        exampleDto.setMessage("Hello world!");
        return exampleDto;
    }

    @PostMapping("shutdown")
    public ExampleDto shutdown() {
        log.info("Got request to shutdown service.");
        new Thread(() -> SpringApplication.exit(configurableApplicationContext, () -> 0)).start();
        ExampleDto exampleDto = new ExampleDto();
        exampleDto.setMessage("Shutdown request accepted");
        return exampleDto;
    }

    @PostMapping("sendReceiveMessageViaTopic")
    public ResponseEntity<Greeting> sendMessage(@RequestParam("message") String message) {
        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("greetings", message, new Greeting(message, new Date()));
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message=[{}] with offset=[{}]", message, result.getRecordMetadata().offset());
            } else {
                log.info("Unable to send message=[{}] due to : {}", message, ex.getMessage());
            }
        });
        Greeting received = greetingsConsumer.getMessageSync();
        if (received != null) {
            return ResponseEntity.ok(received);
        }
        return ResponseEntity.notFound().build();
    }

    @PostMapping("generateUploads")
    public ResponseEntity<?> generateUploads(@RequestParam("rate") int rate, @RequestParam("number") int number) {
        boolean submitted = uploadsGenerator.startGenerator(rate, number);
        if (submitted) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).build();
        }
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).build();
    }

    @PostMapping("resetVerifier")
    public ResponseEntity<?> resetVerifier() {
        if (processedUploadsVerifier != null) {
            processedUploadsVerifier.reset();
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }

    @GetMapping("verifierStatus")
    public ResponseEntity<ProcessedUploadsVerifier.Status> verifierStatus() {
        if (processedUploadsVerifier != null) {
            return ResponseEntity.ok(processedUploadsVerifier.getStatus());
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }
    }


    @Data
    public static class ExampleDto {

        private String message;
    }
}
