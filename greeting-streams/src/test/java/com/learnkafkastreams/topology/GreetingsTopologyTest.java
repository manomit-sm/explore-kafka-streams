package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdeFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GreetingsTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Greeting> inputTopic = null;
    TestOutputTopic<String, Greeting> outputTopic = null;


    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(GreetingsTopology.buildTopology());

        inputTopic = topologyTestDriver.createInputTopic(
                GreetingsTopology.GREETINGS,
                Serdes.String().serializer(),
                SerdeFactory.genericSerde(Greeting.class).serializer()
        );

        outputTopic = topologyTestDriver.createOutputTopic(
                GreetingsTopology.GREETINGS_UPPERCASE,
                Serdes.String().deserializer(),
                SerdeFactory.genericSerde(Greeting.class).deserializer()
        );
    }

    @Test
    void buildTopology() {
        inputTopic.pipeInput("GM", new Greeting("Good Morning", LocalDateTime.now()));

        var count = outputTopic.getQueueSize();

        assertEquals(1, count);
        var outputValue = outputTopic.readKeyValue();
        assertEquals("GOOD MORNING", outputValue.value.message());
        assertNotNull(outputValue.value.timeStamp());

    }

    @Test
    void buildTopologyMultipleInput() {
        inputTopic.pipeKeyValueList(
                List.of(
                        KeyValue.pair("GM", new Greeting("Good Morning", LocalDateTime.now())),
                        KeyValue.pair("GE", new Greeting("Good Evening", LocalDateTime.now()))
                )
        );
        var count = outputTopic.getQueueSize();

        assertEquals(2, count);
        var outputValue = outputTopic.readKeyValuesToList().get(0);
        assertEquals("GOOD MORNING", outputValue.value.message());
        assertNotNull(outputValue.value.timeStamp());

    }

    @Test
    void buildTopologyException() {
        inputTopic.pipeInput("GM", new Greeting("Transient Error", LocalDateTime.now()));
        var count = outputTopic.getQueueSize();
        assertEquals(0, count);
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }
}