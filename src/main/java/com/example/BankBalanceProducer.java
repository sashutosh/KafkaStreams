package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankBalanceProducer {

    public static void main(String[] args) {
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"15.114.180.12:9092");
        producerConfig.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        producerConfig.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");

        producerConfig.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String,String> producer = new KafkaProducer<>(producerConfig);

        int i=0;
        while(true) {
            System.out.println("Producing batch" + i);
            try {
                ProducerRecord<String,String> data1 = getData("john");
                producer.send(data1);
                Thread.sleep(100);

                ProducerRecord<String,String> data2 = getData("stephane");
                producer.send(data2);
                Thread.sleep(100);

                ProducerRecord<String,String> data3 = getData("lucian");
                producer.send(data3);
                Thread.sleep(100);
                i+=1;

            } catch (InterruptedException| IOException e) {
                e.printStackTrace();
                break;
            }

        }
        producer.close();
    }

    public static ProducerRecord<String, String> getData(String customer) throws JsonProcessingException {
        int amount = ThreadLocalRandom.current().nextInt(0,100);
        BankCustomer bankCustomer = new BankCustomer(customer,amount, Instant.now().toString());
        ObjectMapper objectMapper = new ObjectMapper();
        String customerJson = objectMapper.writeValueAsString(bankCustomer);
        return new ProducerRecord<>("bank-transactions",customer,customerJson);
    }

}
