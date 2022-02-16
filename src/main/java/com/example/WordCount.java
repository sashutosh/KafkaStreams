package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCount {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-app1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"15.114.180.12:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        Topology topology = createTopology();

        KafkaStreams streams = new KafkaStreams(topology,config);

        streams.start();

        System.out.println(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }

    }

    public static Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        //1. Stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        //2. map values to lowercase
        final Pattern pattern = Pattern.compile("\\W+");

        KStream<String, Long> counts = wordCountInput
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<>(value, value))
                .filter((key, value) -> (!value.equals("the")))
                .groupByKey()
                .count()
                .toStream();

        counts.to("wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
