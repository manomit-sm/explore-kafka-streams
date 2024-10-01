package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class ExploreKTableTopology {

    public static String WORDS = "words";
    public static String WORDS_OUTPUT = "words_output";

    public static Topology buildKTableTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var wordsTable = streamsBuilder.table(WORDS,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("words-store")
        );
        wordsTable.filter(
                (key, value) -> value.length() > 2
        ).toStream()
                .to(WORDS_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
                //.print(Printed.<String, String>toSysOut().withLabel("words-k-table"));

        /*var wordsGlobalTable = streamsBuilder.globalTable(WORDS,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("words-store")
        ); */

        return streamsBuilder.build();
    }
}
