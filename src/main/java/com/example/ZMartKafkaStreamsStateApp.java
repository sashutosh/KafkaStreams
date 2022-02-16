package com.example;

import com.example.clients.MockDataProducer;
import com.example.model.Purchase;
import com.example.model.PurchasePattern;
import com.example.model.RewardAccumulator;
import com.example.partitioner.RewardsStreamPartitioner;
import com.example.serdes.StreamsSerdes;
import com.example.transformer.PurchaseRewardTransformer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMartKafkaStreamsStateApp {

    public static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsStateApp.class);
    public static void main(String[] args) {
        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,Purchase> purchaseKStream = builder.stream( "transactions", Consumed.with(stringSerde, purchaseSerde))
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        String rewardStatesStoreName = "rewardsPointStore";
        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardStatesStoreName);
        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());
        builder.addStateStore(storeBuilder);

        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();

        KStream<String, Purchase> partitionedStream = purchaseKStream.through("customer-transaction", Produced.with(stringSerde, purchaseSerde, streamPartitioner));
       // partitionedStream.print(Printed.<String,Purchase>toSysOut().withLabel("customer-transaction"));

        KStream<String, RewardAccumulator> statefulRewardsAccumulator = partitionedStream.transformValues(() -> new PurchaseRewardTransformer(rewardStatesStoreName), rewardStatesStoreName);

        statefulRewardsAccumulator.to("rewards",Produced.with(stringSerde,rewardAccumulatorSerde));
        statefulRewardsAccumulator.print(Printed.<String,RewardAccumulator>toSysOut().withLabel("rewards"));

        LOG.info("Starting Adding State Example");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        LOG.info("ZMart Adding State Application Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        try {
            Thread.sleep(65000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Shutting down the Add State Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();


    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "AddingStateConsumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "AddingStateGroupId");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AddingStateAppId");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "15.114.180.12:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
