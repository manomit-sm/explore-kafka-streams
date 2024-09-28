package com.learnkafkastreams.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {

    public static <T> Serde<T> genericSerde(Class<T> tClass) {
        MyJsonSerializer<T> jsonSerializer =  new MyJsonSerializer<>();
        MyJsonDeSerializer<T> jsonDeSerializer = new MyJsonDeSerializer<>(tClass);
        return Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }
}
