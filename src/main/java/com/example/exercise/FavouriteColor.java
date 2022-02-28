package com.example.exercise;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColor {

    public static Logger LOG = LoggerFactory.getLogger(FavouriteColor.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Fav-colors-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Fav-colors");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Fav-colors-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "15.114.180.12:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsConfig streamsConfig = new StreamsConfig(props);
        StreamsBuilder streamsBuilder = new StreamsBuilder();


        List<String> validColor = Arrays.asList("red", "green", "blue");
        KStream<String, String> colors = streamsBuilder.stream("color", Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value.contains(","))
                .map((key, value) -> KeyValue.pair(value.split(",")[0], value.split(",")[1]))
                .filter((s, s2) -> validColor.contains(s2));
        colors.print(Printed.<String, String>toSysOut().withLabel("colors"));
        colors.to("colorOrdered", Produced.with(Serdes.String(), Serdes.String()));
        //colors.to("colorOutput", Produced.with(Serdes.String(),Serdes.Long()));


        KTable<String, Long> colorOrdered = streamsBuilder.table("colorOrdered", Consumed.with(Serdes.String(), Serdes.String()))
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        LOG.info("ZMart First Kafka Streams Application Started");
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }
}
