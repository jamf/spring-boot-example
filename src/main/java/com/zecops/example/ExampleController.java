package com.zecops.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ExampleController {

    private static final Logger logger = LoggerFactory.getLogger(ExampleController.class);
    private final ConfigurableApplicationContext configurableApplicationContext;

    @Autowired
    public ExampleController(ConfigurableApplicationContext configurableApplicationContext) {
        this.configurableApplicationContext = configurableApplicationContext;
    }

    @GetMapping()
    public ExampleDto helloWorldFromMemory() {
        logger.info("Got request to return Hello world.");
        ExampleDto exampleDto = new ExampleDto();
        exampleDto.setMessage("Hello world!");
        return exampleDto;
    }

    @PostMapping("shutdown")
    public ExampleDto shutdown() {
        logger.info("Got request to shutdown service.");
        new Thread(() -> SpringApplication.exit(configurableApplicationContext, () -> 0)).start();
        ExampleDto exampleDto = new ExampleDto();
        exampleDto.setMessage("Shutdown request accepted");
        return exampleDto;
    }


    public static class ExampleDto {

        private String message;

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
