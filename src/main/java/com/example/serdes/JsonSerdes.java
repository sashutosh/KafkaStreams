package com.example.serdes;

import com.example.BankCustomer;
import com.example.CustomerTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class JsonSerdes {
    public JsonSerdes() {
    }

    public static Serde<BankCustomer> bankCustomerSerde() {
        JsonSerializer<BankCustomer> serializer = new JsonSerializer<>();
        JsonDeserializer<BankCustomer> deserializer = new JsonDeserializer<>(BankCustomer.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

    public static Serde<CustomerTransaction> customerTransactionSerde() {
        JsonSerializer<CustomerTransaction> serializer = new JsonSerializer<>();
        JsonDeserializer<CustomerTransaction> deserializer = new JsonDeserializer<>(CustomerTransaction.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}
