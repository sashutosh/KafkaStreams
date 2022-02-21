package com.example;

import com.example.clients.MockDataProducer;
import com.example.joiner.PurchaseJoiner;
import com.example.model.CorrelatedPurchase;
import com.example.model.Purchase;
import com.example.serdes.StreamsSerdes;
import com.example.timestampextractor.TransactionTimestampExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class KafkaStreamsJoin {

    public static final Logger LOG = LoggerFactory.getLogger("KafkaStreamsJoin");

    public static void main(String[] args) {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        StreamsBuilder builder = new StreamsBuilder();

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        KeyValueMapper<String, Purchase, KeyValue<String, Purchase>> custIdCCMasking = (k, v) -> {
            Purchase masked = Purchase.builder(v).maskCreditCard().build();
            return new KeyValue<>(masked.getCustomerId(), masked);
        };


        Predicate<String, Purchase> coffeePurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> electronicPurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");


        int COFFEE_PURCHASE = 0;
        int ELECTRONICS_PURCHASE = 1;

        KStream<String, Purchase> transactionStream = builder.stream("transactions", Consumed.with(Serdes.String(), purchaseSerde)).map(custIdCCMasking);

        KStream<String, Purchase>[] branchesStream = transactionStream.selectKey((k, v) -> v.getCustomerId()).branch(coffeePurchase, electronicPurchase);

        KStream<String, Purchase> coffeeStream = branchesStream[COFFEE_PURCHASE];
        KStream<String, Purchase> electronicsStream = branchesStream[ELECTRONICS_PURCHASE];

        coffeeStream.print(Printed.<String, Purchase>toSysOut().withLabel("coffee KStream"));


        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();
        JoinWindows twentyMinuteWindow = JoinWindows.of(Duration.ofSeconds(60 * 20));
        KStream<String, CorrelatedPurchase> joinedKStream = coffeeStream.join(electronicsStream, purchaseJoiner, twentyMinuteWindow, StreamJoined.with(stringSerde, purchaseSerde, purchaseSerde));
        joinedKStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("joined KStream"));

        MockDataProducer.producePurchaseData();

        LOG.info("Starting Join Examples");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.start();
        try {
            Thread.sleep(65000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Shutting down the Join Examples now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "15.114.180.12:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class);
        return props;
    }
}
