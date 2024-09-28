package com.learnkafkastreams.launcher;

import com.learnkafkastreams.exceptions.GreetingDeserializationExceptionHandler;
import com.learnkafkastreams.exceptions.GreetingStreamProcessorExceptionHandler;
import com.learnkafkastreams.exceptions.GreetingStreamSerializationExceptionHandler;
import com.learnkafkastreams.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.learnkafkastreams.topology.GreetingsTopology.*;

@Slf4j
public class GreetingsStreamApp {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Runtime.getRuntime().availableProcessors());
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, GreetingDeserializationExceptionHandler.class);
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, GreetingStreamSerializationExceptionHandler.class);
        createTopics(properties, List.of(GREETINGS, GREETINGS_UPPERCASE, GREETINGS_SPANISH));
        try (var kafkaStreams = new KafkaStreams(GreetingsTopology.buildTopology(), properties)) {
            kafkaStreams.setUncaughtExceptionHandler(new GreetingStreamProcessorExceptionHandler());
            kafkaStreams.start();
        } catch (Exception exception) {
            log.error("Exception {}", exception.getMessage(), exception);
            throw new RuntimeException(exception.getMessage());
        }
    }

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 2;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
           createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
}
