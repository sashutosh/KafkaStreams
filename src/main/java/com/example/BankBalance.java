package com.example;

import com.example.serdes.JsonSerdes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.Properties;

public class BankBalance {

    public static void main(String[] args) {
        Properties kafkaConfig = new Properties();
        kafkaConfig.put(StreamsConfig.APPLICATION_ID_CONFIG,"bank-balance-app");
        kafkaConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"15.114.180.12:9092");
        kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        kafkaConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG,StreamsConfig.EXACTLY_ONCE_V2);

       /*kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
         kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,JsonSerdes.bankCustomerSerde().getClass());*/

        StreamsBuilder builder = new StreamsBuilder();
        //1. Stream from Kafka
        KStream<String, BankCustomer> bankTransactions = builder.stream("bank-transactions" ,Consumed.with(Serdes.String(), JsonSerdes.bankCustomerSerde()));

        CustomerTransaction ct = new CustomerTransaction(0,0, Instant.ofEpochMilli(0).toString());

        KStream<String, CustomerTransaction> customerTransactionKStream = bankTransactions
                .groupByKey(/*Grouped.with(Serdes.String(), JsonSerdes.bankCustomerSerde())*/)
                .aggregate(() -> ct,
                        (aggKey, transaction, balance) -> newBalance(transaction, balance))
                .toStream();

        customerTransactionKStream.to("bank-balance-once", Produced.with(Serdes.String(), JsonSerdes.customerTransactionSerde()));

        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaConfig);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static CustomerTransaction newBalance(BankCustomer transaction, CustomerTransaction currentBalance) {
        currentBalance.balance= currentBalance.balance + transaction.amount;
        currentBalance.count+=1;
        currentBalance.time= transaction.time;
        return new CustomerTransaction(currentBalance.count, currentBalance.balance, currentBalance.time);
    }


}
