package com.zecops.example.streams;


import com.zecops.example.dto.Upload;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * This approach with Kafka streams achieves Exactly-Once-Semantics when tested with killing the application and killing the broker.
 * Unhandled exceptions during processing however caused incorrectness.
 * Bad-case performance is much worse than regular consumer-producer since tasks re-sync and restarts take much time.
 *
 */
@Slf4j
@Configuration
@EnableKafkaStreams
public class StreamsConfiguration {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;
    @Value(value = "${spring.kafka.client-id}")
    private String clientId;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public StreamsConfiguration(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "upload-streams-processor-app");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringDeserializationExceptionHandler.class);
        props.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, recoverer());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        return new KafkaStreamsConfiguration(props);
    }


    @Bean
    public DeadLetterPublishingRecoverer recoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> new TopicPartition("recovererDLQ", -1));
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer configurer() {
        return streamsBuilderFactoryBean -> {
            streamsBuilderFactoryBean.setStateListener((newState, oldState) -> log.info("State transition from " + oldState + " to " + newState));
            streamsBuilderFactoryBean.setStreamsUncaughtExceptionHandler(exception -> {
                log.error("Exception in streams: {}", exception.getMessage(), exception);
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
            });
        };
    }

    @Bean
    public KStream<?, ?> kStream(StreamsBuilder kStreamBuilder) {
        KStream<String, Upload> stream = kStreamBuilder.stream("uploads-stream", Consumed.with(Serdes.String(), new JsonSerde<>(Upload.class)).withName("uploads-processor-streams").withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));
        stream.mapValues(StreamsConfiguration::processUpload)
                .to("uploads-stream-processed", Produced.with(Serdes.String(), new JsonSerde<>(Upload.class)));
        return stream;
    }

    private static Upload processUpload(Upload in) {
        Random random = ThreadLocalRandom.current();
        /*if (random.nextInt(10) == 0) {
            throw new RuntimeException("Streams - Simulated failure 1");
        }*/
        try {
            Thread.sleep(random.nextInt(5000));
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
        /*if (random.nextInt(10) == 0) {
            throw new RuntimeException("Streams - Simulated failure 2");
        }*/
        in.setProcessed(true);
        return in;
    }

    @Bean
    public KStream<?, ?> kStreamVerifier(StreamsBuilder kStreamBuilder) {
        KStream<String, Upload> stream = kStreamBuilder.stream("uploads-stream-processed", Consumed.with(Serdes.String(), new JsonSerde<>(Upload.class)).withName("streams-verifier").withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST));

        stream.filter((key, value) -> value.isProcessed())
                .groupByKey()
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("upload-counts-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .groupBy((key, value) -> new KeyValue<>(value == 1 ? "Unique" : "Duplicated", 0L), Grouped.<String, Long>as("u-d-grouping").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("u-d-counts-store").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                .toStream()
                .foreach((key, value) -> log.info("Last counts state: {}: {}", key, value));

        return stream;
    }
}
