package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.serdes.SerdeFactory;
import com.learnkafkastreams.utils.OrderTimestampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String STORES = "stores";

    public static final String GENERAL_ORDERS = "general_orders";

    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String RESTAURANT_ORDERS = "general_orders";

    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";

    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";

    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";

    public static final String GENERAL_ORDERS_REVENUE_WINDOW = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS_REVENUE_WINDOW = "restaurant_orders_revenue_window";


    public static Topology topology() {

        ValueMapper<Order, Revenue> revenueValueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, Order> orderStream = streamsBuilder
                .stream(
                        ORDERS,
                        Consumed.with(Serdes.String(), SerdeFactory.genericSerde(Order.class))
                                .withTimestampExtractor(new OrderTimestampExtractor())

                );
        // .selectKey((key, value) -> value.locationId()); // it's an expensive operation;
        orderStream.print(Printed.<String, Order>toSysOut().withLabel("orders"));
        // Join between KStream to KTable
        final KTable<String, Store> storeTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), SerdeFactory.genericSerde(Store.class))
                );
        orderStream
                .split(Named.as("restaurant-stream"))
                .branch((key, order) -> order.orderType().equals(OrderType.GENERAL), Branched.withConsumer(generalStream -> {
                    /*generalStream
                            .mapValues((key, order) -> revenueValueMapper.apply(order))
                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdeFactory.genericSerde(Revenue.class))); */
                    aggregateOrderByCount(generalStream, GENERAL_ORDERS_COUNT);
                    aggregateOrderByRevenue(generalStream, GENERAL_ORDERS_REVENUE, storeTable);
                    aggregateOrderByRevenueByTimeWindow(generalStream, GENERAL_ORDERS_REVENUE_WINDOW, storeTable);
                }))
                .branch((key, order) -> order.orderType().equals(OrderType.RESTAURANT), Branched.withConsumer(restaurantStream -> {
                    /* restaurantStream
                            .mapValues((key, order) -> revenueValueMapper.apply(order))
                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdeFactory.genericSerde(Revenue.class))) */
                    aggregateOrderByCount(restaurantStream, RESTAURANT_ORDERS_COUNT);
                    aggregateOrderByRevenue(restaurantStream, RESTAURANT_ORDERS_REVENUE, storeTable);
                    aggregateOrderByRevenueByTimeWindow(restaurantStream, RESTAURANT_ORDERS_REVENUE_WINDOW, storeTable);

                }));
        return streamsBuilder.build();
    }

    private static void aggregateOrderByRevenueByTimeWindow(KStream<String, Order> generalStream, String storeName, KTable<String, Store> storeTable) {


        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(15));
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator =
                (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order);

        final KTable<Windowed<String>, TotalRevenue> revenueKTable = generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdeFactory.genericSerde(Order.class)))
                .windowedBy(timeWindows)
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>
                                        as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdeFactory.genericSerde(TotalRevenue.class))

                );
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
        final KStream<String, TotalRevenueWithAddress> revenueWithStoreTable = revenueKTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storeTable, valueJoiner, Joined.with(Serdes.String(), SerdeFactory.genericSerde(TotalRevenue.class), SerdeFactory.genericSerde(Store.class)));
        revenueWithStoreTable
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName + "byStore"));
    }

    private static void aggregateOrderByRevenue(KStream<String, Order> generalStream, String storeName, KTable<String, Store> storeTable) {



        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator =
                (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order);

        final KTable<String, TotalRevenue> revenueKTable = generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdeFactory.genericSerde(Order.class)))
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>
                                        as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdeFactory.genericSerde(TotalRevenue.class))

                );
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
        final KTable<String, TotalRevenueWithAddress> revenueWithStoreTable = revenueKTable
                .join(storeTable, valueJoiner);
        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName + "byStore"));
    }

    private static void aggregateOrderByCount(KStream<String, Order> generalStream, String storeName) {

        final KTable<String, Long> ordersCountPerStore = generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))

                .groupByKey(Grouped.with(Serdes.String(), SerdeFactory.genericSerde(Order.class)))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore.toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));
    }
}
