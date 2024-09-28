package com.learnkafkastreams.exceptions;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class GreetingDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private final AtomicInteger counter = new AtomicInteger(0);
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext processorContext, ConsumerRecord<byte[], byte[]> consumerRecord, Exception e) {
        log.error(
                "Exception is {}, and teh Kafka record is {}",
                e.getMessage(), consumerRecord, e
        );
        if (counter.get() < 2) {
            counter.incrementAndGet();
            return DeserializationHandlerResponse.CONTINUE;
        }
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
