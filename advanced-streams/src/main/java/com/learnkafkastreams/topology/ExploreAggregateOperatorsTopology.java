package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> inputStream = streamsBuilder.stream(
                AGGREGATE,
                Consumed.with(Serdes.String(), Serdes.String())
        );
        inputStream.print(Printed.<String, String>toSysOut().withLabel(AGGREGATE));

        final KGroupedStream<String, String> groupedStream = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String())); //we can't change the key
                       // .groupBy((key, value) -> value, Grouped.with(Serdes.String(), Serdes.String())); // we can change the key. It will create re-partition and create new changelog internal topic
        exploreCount(groupedStream);
        exploreReduce(groupedStream);
        exploreAggregate(groupedStream);
        return streamsBuilder.build();
    }

    private static void exploreAggregate(KGroupedStream<String, String> groupedStream) {
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer =
                AlphabetWordAggregate::new;
        Aggregator<String, String, AlphabetWordAggregate> aggregator =
                (key, value, aggregate) -> aggregate.updateNewEvents(key, value);
        final KTable<String, AlphabetWordAggregate> aggregateKTable = groupedStream
                .aggregate(
                        alphabetWordAggregateInitializer,
                        aggregator,
                        Materialized
                                .<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>as("aggregated-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.alphabetWordAggregate())
                );
        aggregateKTable
                .toStream()
                .print(Printed.<String, AlphabetWordAggregate>toSysOut().withLabel("aggregates-words"));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedStream) {
        final KTable<String, String> reduce = groupedStream
                .reduce((val1, val2) -> val1.toUpperCase() + "-" + val2.toUpperCase());

        reduce.toStream()
                .print(Printed.<String, String>toSysOut().withLabel("reduced-per-key"));
    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {
        final KTable<String, Long> count = groupedStream
                //.count(Named.as("count-per-key"));
                        .count(
                                Named.as("count-per-key"),
                                Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("count-per-key")
                                        .withKeySerde(Serdes.String())
                                        .withValueSerde(Serdes.Long())
                        );
        count.toStream().print(Printed.<String, Long>toSysOut().withLabel("words-count-per-key"));
    }

}
