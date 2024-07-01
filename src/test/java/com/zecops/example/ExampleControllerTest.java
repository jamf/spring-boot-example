package com.zecops.example;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = ExampleApplication.class)
@EnableAutoConfiguration(exclude = {UserDetailsServiceAutoConfiguration.class})
@TestPropertySource("classpath:application-test.properties") // Spring gives priority to properties outside the jar regardless of profile, so need to explicitly set this.
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExampleControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;


    @Test
    void apiGetTest() {
        ResponseEntity<ExampleController.ExampleDto> response = restTemplate.getForEntity("/", ExampleController.ExampleDto.class);
        Assertions.assertEquals(HttpStatusCode.valueOf(200), response.getStatusCode());
        Assertions.assertEquals("Hello world!", response.getBody().getMessage());
    }
}
