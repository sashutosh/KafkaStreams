package com.example.transformer;

import com.example.model.Purchase;
import com.example.model.RewardAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class PurchaseRewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {

    private KeyValueStore<String,Integer> stateStore;
    String storeName;
    private ProcessorContext context;

    public PurchaseRewardTransformer(String rewardStatesStoreName) {
        this.storeName=rewardStatesStoreName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
        this.context=processorContext;
        stateStore = this.context.getStateStore(storeName);
    }

    @Override
    public RewardAccumulator transform(Purchase purchase) {
        RewardAccumulator rewardAccumulator = RewardAccumulator.builder(purchase).build();
        Integer accumulatedSoFar = stateStore.get(rewardAccumulator.getCustomerId());

        if(accumulatedSoFar!=null){
            rewardAccumulator.addRewardPoints(accumulatedSoFar);
        }

        stateStore.put(rewardAccumulator.getCustomerId(), rewardAccumulator.getTotalRewardPoints());
        return rewardAccumulator;
    }

    @Override
    public void close() {

    }
}
