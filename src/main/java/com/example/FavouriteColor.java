package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColor {

    public static void main(String[] args) {

        Properties kafkaConfig = new Properties();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"favourite-color-app");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"15.114.180.12:9092");
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        List<String> validValues = Arrays.asList("red", "green","blue");
        StreamsBuilder builder = new StreamsBuilder();
        //1. Stream from Kafka
        KStream<String, String> colors = builder.stream("color-input");

        /*KStream colorCountStream = colors
                .filter((key,value) -> validValues.contains(value))
                .map((name, color) -> new KeyValue<Object, Object>(color, color))
                .groupByKey()
                .count()
                .toStream();
*/
        KStream colorCountStream = colors
                .filter((key,value) -> validValues.contains(value))
                .groupBy((user,color)-> new KeyValue<>(color,color))
                .count()
                .toStream();


        colorCountStream.to("color-output1", Produced.with(Serdes.String(),Serdes.Long()));
        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
