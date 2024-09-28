package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdeFactory {

    public static Serde<Greeting> greetingSerde() {
        return new GreetingSerde();
    }

    public static <T> Serde<T> genericSerde(Class<T> tClass) {
        MyJsonSerializer<T> jsonSerializer =  new MyJsonSerializer<>();
        MyJsonDeSerializer<T> jsonDeSerializer = new MyJsonDeSerializer<>(tClass);
        return Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }
}
