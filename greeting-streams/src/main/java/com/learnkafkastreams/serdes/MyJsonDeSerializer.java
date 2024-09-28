package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class MyJsonDeSerializer<T> implements Deserializer<T> {

    private final Class<T> destClass;
    private ObjectMapper objectMapper;

    public MyJsonDeSerializer(Class<T> destClass) {
        this.destClass = destClass;
        this.objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }
    @Override
    public T deserialize(String s, byte[] bytes) {
        if (bytes == null)
            return null;
        try {
            return objectMapper.readValue(bytes, destClass);
        } catch (IOException e) {
            log.error("IOException {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
