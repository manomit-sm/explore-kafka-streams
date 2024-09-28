package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@RequiredArgsConstructor
@Slf4j
public class GreetingDeSerializer implements Deserializer<Greeting> {

    private final ObjectMapper objectMapper;
    @Override
    public Greeting deserialize(String topic, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Greeting.class);
        } catch (IOException e) {
            log.error("IOException {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
