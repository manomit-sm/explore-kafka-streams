package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;


@RequiredArgsConstructor
@Slf4j
public class GreetingSerializer implements Serializer<Greeting> {

    private final ObjectMapper objectMapper;
    @Override
    public byte[] serialize(String topic, Greeting greeting) {
        try {
            return objectMapper.writeValueAsBytes(greeting);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
