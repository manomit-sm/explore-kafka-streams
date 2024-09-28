package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVIATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        
        joinKStreamWithKTable(streamsBuilder);
        joinKStreamWithGlobalKTable(streamsBuilder);
        joinKTableWithKTable(streamsBuilder);
        joinKStreamWithKStream(streamsBuilder);
        
        return streamsBuilder.build();
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {
        final KStream<String, String> alphabetStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> alphabetAbbrStream = streamsBuilder
                .stream(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;


        final KStream<String, Alphabet> join = alphabetAbbrStream
                //.leftJoin
                //.outerJoin
                .join(
                        alphabetStream,
                        valueJoiner,
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()).withName("alphabets-join").withStoreName("alphabets-join")
                );
        join.print(Printed.<String, Alphabet>toSysOut().withLabel("k-stream-to-k-stream"));

    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {

        final KStream<String, String> alphabetStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        final KTable<String, String> alphabetTable = streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabet-store"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        final KStream<String, Alphabet> join = alphabetStream
                .join(alphabetTable, valueJoiner);
        join.print(Printed.<String, Alphabet>toSysOut().withLabel("inner-join"));


    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {

        final KStream<String, String> alphabetStream = streamsBuilder
                .stream(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        final GlobalKTable<String, String> alphabetTable = streamsBuilder
                .globalTable(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabet-store"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;
        KeyValueMapper<String, String, String> keyValueMapper = (leftKey, rightKey) -> leftKey;
        final KStream<String, Alphabet> join = alphabetStream
                .join(alphabetTable, keyValueMapper,valueJoiner);
        join.print(Printed.<String, Alphabet>toSysOut().withLabel("inner-join-global-k-table"));


    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {

        final KTable<String, String> alphabetStream = streamsBuilder
                .table(ALPHABETS_ABBREVIATIONS, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabet"));

        final KTable<String, String> alphabetTable = streamsBuilder
                .table(ALPHABETS, Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("alphabet-store"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;
        final KTable<String, Alphabet> join = alphabetStream
                .join(alphabetTable,valueJoiner);
        join.toStream().print(Printed.<String, Alphabet>toSysOut().withLabel("inner-join-k-table-to-k-table"));


    }

}
