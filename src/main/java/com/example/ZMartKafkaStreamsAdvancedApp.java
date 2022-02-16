package com.example;

import com.example.clients.MockDataProducer;
import com.example.model.Purchase;
import com.example.model.PurchasePattern;
import com.example.model.RewardAccumulator;
import com.example.serdes.StreamsSerdes;
import com.example.service.SecurityDBService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ZMartKafkaStreamsAdvancedApp {

    private static final Logger LOG = LoggerFactory.getLogger("ZMartKafkaStreamsAdvancedApp");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "AdvancedZmart-Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "zmart-advanced-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "AdvancedZmart-Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "15.114.180.12:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);


        StreamsConfig streamsConfig = new StreamsConfig(props);
        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String,Purchase> purchaseKStream = streamsBuilder.stream("transactions", Consumed.with(Serdes.String(),purchaseSerde))
                .mapValues(p-> Purchase.builder(p).maskCreditCard().build());

        /*KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.print( Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde,purchasePatternSerde));


        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards"));
        rewardsKStream.to("rewards", Produced.with(stringSerde,rewardAccumulatorSerde));
*/
        KStream<Long, Purchase> filteredPurchaseKStream = purchaseKStream
                .filter((key, purchase) -> purchase.getPrice() > 5.00)
                .selectKey((key, purchase) -> purchase.getPurchaseDate().getTime());

        filteredPurchaseKStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchases"));
        filteredPurchaseKStream.to("purchases", Produced.with(Serdes.Long(),purchaseSerde));

        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        KStream<String, Purchase>[] kStreamByDept = purchaseKStream.branch(isCoffee, isElectronics);

        kStreamByDept[0].to("coffee", Produced.with(stringSerde,purchaseSerde));
        kStreamByDept[0].print(Printed.<String,Purchase>toSysOut().withLabel("coffee"));

        kStreamByDept[1].to("electronics", Produced.with(stringSerde,purchaseSerde));
        kStreamByDept[1].print(Printed.<String,Purchase>toSysOut().withLabel("electronics"));

        ForeachAction<String,Purchase> purchaseForeachAction = (key,purchase)-> SecurityDBService.saveRecord(purchase.getPurchaseDate(),purchase.getEmployeeId(), purchase.getItemPurchased());

        purchaseKStream.filter((key,purchase)-> purchase.getEmployeeId().equals("000000"));

        MockDataProducer.producePurchaseData();

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),streamsConfig);
        LOG.info("ZMart Advanced Requirements Kafka Streams Application Started");
        kafkaStreams.start();
        try {
            Thread.sleep(65000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }
}
