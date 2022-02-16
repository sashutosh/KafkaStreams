package com.example.partitioner;

import com.example.model.Purchase;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class RewardsStreamPartitioner implements StreamPartitioner<String, Purchase> {
    @Override
    public Integer partition(String s, String key, Purchase value, int numPartitions) {
        return value.getCustomerId().hashCode()%numPartitions;
    }
}
