package com.learnkafkastreams.topology;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.*;
import java.time.format.DateTimeFormatter;

@Slf4j
public class ExploreWindowTopology {

    public static final String WINDOW_WORDS = "windows-words";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> wordsStream = streamsBuilder
                .stream(
                        WINDOW_WORDS,
                        Consumed.with(Serdes.String(), Serdes.String())
                );
        // tumblingWindows(wordsStream);
        // hoppingWindows(wordsStream);
        slidingWindows((wordsStream));
        return streamsBuilder.build();
    }

    private static void tumblingWindows(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize);

        final KTable<Windowed<String>, Long> count = wordsStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(timeWindows)
                .count()
                .suppress(
                    Suppressed
                            .untilWindowCloses(
                                    Suppressed
                                            .BufferConfig
                                            .unbounded()
                                            .shutDownWhenFull()
                            )
                )
                ;
        count.toStream()
                .peek((key, value) -> {
                    log.info("Tumbling Window Key {}, Value {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumbling-window"));
    }

    private static void hoppingWindows(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(Duration.ofSeconds(3));

        final KTable<Windowed<String>, Long> count = wordsStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(timeWindows)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(
                                        Suppressed
                                                .BufferConfig
                                                .unbounded()
                                                .shutDownWhenFull()
                                )
                )
                ;
        count.toStream()
                .peek((key, value) -> {
                    log.info("Hopping Window Key {}, Value {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumbling-window"));
    }

    private static void slidingWindows(KStream<String, String> wordsStream) {
        Duration windowSize = Duration.ofSeconds(5);
        var slidingWindow = SlidingWindows.ofTimeDifferenceWithNoGrace(windowSize);

        final KTable<Windowed<String>, Long> count = wordsStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(slidingWindow)
                .count()
                .suppress(
                        Suppressed
                                .untilWindowCloses(
                                        Suppressed
                                                .BufferConfig
                                                .unbounded()
                                                .shutDownWhenFull()
                                )
                )
                ;
        count.toStream()
                .peek((key, value) -> {
                    log.info("Sliding Window Key {}, Value {}", key, value);
                    printLocalDateTimes(key, value);
                })
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel("tumbling-window"));
    }


    private static void printLocalDateTimes(Windowed<String> key, Long value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();

        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }

}
