package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExploreKTableTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, String> inputTopic = null;
    TestOutputTopic<String, String> outputTopic = null;

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(ExploreKTableTopology.buildKTableTopology());

        inputTopic =
                topologyTestDriver.
                        createInputTopic(
                                ExploreKTableTopology.WORDS, Serdes.String().serializer(),
                                Serdes.String().serializer());

        outputTopic =
                topologyTestDriver
                        .createOutputTopic(
                                ExploreKTableTopology.WORDS_OUTPUT,
                                Serdes.String().deserializer(),
                                Serdes.String().deserializer());
    }

    @Test
    void buildTopology() {
        inputTopic.pipeInput("A", "Apple");
        inputTopic.pipeInput("A", "Air");
        inputTopic.pipeInput("B", "Bus");
        inputTopic.pipeInput("B", "Ball");

        assertEquals(4, outputTopic.getQueueSize()); // It should have been 2, because of KTable. But TopologyTestDriver has this limitation. It don't have caching behaviour. We need Kafka Cluster like EmbeddedKafka
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }



}
