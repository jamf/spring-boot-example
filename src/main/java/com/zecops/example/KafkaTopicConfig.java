package com.zecops.example;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        KafkaAdmin kafkaAdmin = new KafkaAdmin(configs);
        // If we want to stop Spring startup in case Kafka is not available
        kafkaAdmin.setFatalIfBrokerNotAvailable(true);
        return kafkaAdmin;
    }

    @Bean
    public NewTopic greetingsTopic() {
        return new NewTopic("greetings", 1, (short) 1);
    }

    @Bean
    public NewTopic uploadsTopic() {
        return new NewTopic("uploads", 5, (short) 1);
    }

    @Bean
    public NewTopic uploadsProcessesTopic() {
        return new NewTopic("uploads-processed", 5, (short) 1);
    }
}