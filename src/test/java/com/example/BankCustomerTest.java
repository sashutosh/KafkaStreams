package com.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BankCustomerTest {

    @Test
    public void customerCreationTest() throws JsonProcessingException {
        ProducerRecord<String, String> producerRecord = BankBalanceProducer.getData("john");
        Assertions.assertEquals(producerRecord.key(),"john");
    }
}
