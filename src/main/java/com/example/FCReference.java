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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class FCReference {

    public static void main(String[] args) {
        Properties kafkaConfig = new Properties();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"favourite-color-app2");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"15.114.180.12:9092");
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        List<String> validColors = Arrays.asList("red", "green","blue");
        StreamsBuilder builder = new StreamsBuilder();
        //1. Stream from Kafka
        KStream<String, String> textLines = builder.stream("color-input2");

        textLines
                .filter((key,value)-> value.contains(","))
                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
                .mapValues(value-> value.split(",")[1].toLowerCase())
                .filter((user,color)->validColors.contains(color))
                .to("user-color");

        KTable<String, String> userAndColorsTable = builder.table("user-color");
        KTable<String, Long> favouriteColors = userAndColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();
        KStream<String,Long> favouriteColorsStream= favouriteColors.toStream();

        favouriteColorsStream.to("color-output2", Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaConfig);

        streams.cleanUp();

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
