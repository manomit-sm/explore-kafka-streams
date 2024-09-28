package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.GreetingSerde;
import com.learnkafkastreams.serdes.SerdeFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsTopology {

    public static final String GREETINGS = "greetings";

    public static final String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static final String GREETINGS_SPANISH = "greetings_spanish";

    public static Topology buildTopology() {
        var streamsBuilder = new StreamsBuilder();
        KStream<String, Greeting> greetingsStream = streamsBuilder
                //.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()))
                .stream(GREETINGS, Consumed.with(Serdes.String(), SerdeFactory.genericSerde(Greeting.class)));
        final KStream<String, Greeting> greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), SerdeFactory.genericSerde(Greeting.class)));
        greetingsStream
                .filter((s, s2) -> s2.message().length() > 5)
                .peek((s, s2) -> log.info("Filtered Key {}, Value {}", s, s2))
                //.filterNot((s, s2) -> s2.length() > 5)
                // .print(Printed.<String, String>toSysOut().withLabel("greetingStream"))
                //.map((key ,  value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))
                /*.flatMap((key, value) -> {
                    var newValues = Arrays.asList(value.split(""));
                    return newValues.stream().map(t -> KeyValue.pair(key.toUpperCase(), t)).toList();
                })
                .flatMapValues((key, value) -> {
                    var newValues = Arrays.asList(value.split(""));
                    return newValues.stream().map(String::toUpperCase).collect(Collectors.toList());
                })*/
                .mapValues((s, s2) -> {
                    if (s2.message().equals("Transient Error")) {
                        try {
                            throw new IllegalArgumentException(s2.message());
                        } catch (Exception ex) {
                            return null;
                        }
                    }
                    return new Greeting(s2.message().toUpperCase(), s2.timeStamp());
                }).filter((key, value) -> key != null && value != null)
                .merge(greetingsSpanishStream)
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdeFactory.genericSerde(Greeting.class)));
        return streamsBuilder.build();

    }
}
