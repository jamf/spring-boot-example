package com.zecops.example;

import com.fasterxml.jackson.core.StreamReadConstraints;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class ExampleApplication {

    public static void main(String[] args) {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> log.error("Uncaught exception in thread '{}'", t.getName(), e));
        StreamReadConstraints.overrideDefaultStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(100_000_000).build());
        SpringApplication.run(ExampleApplication.class, args);
    }
}
