package com.zecops.example;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    @Value(value = "${spring.kafka.client-id}")
    private String clientId;
    @Value("${spring.kafka.producer.transaction-id-prefix}")
    private String transactionPrefix;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        DefaultKafkaProducerFactory<String, Object> defaultKafkaProducerFactory = new DefaultKafkaProducerFactory<>(configProps);
        defaultKafkaProducerFactory.setMaxAge(Duration.ofDays(5)); // transactional.id.expiration.ms is 7 days by default
        // This is required in spite of the setting in application.properties, which is by itself required to auto enable KafkaTransactionManager.  https://stackoverflow.com/a/73363860
        defaultKafkaProducerFactory.setTransactionIdPrefix(transactionPrefix);
        return defaultKafkaProducerFactory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setAllowNonTransactional(true);
        return kafkaTemplate;
    }

}