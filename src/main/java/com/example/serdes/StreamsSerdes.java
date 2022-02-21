package com.example.serdes;

import com.example.collectors.FixedSizePriorityQueue;
import com.example.model.Purchase;
import com.example.model.PurchasePattern;
import com.example.model.RewardAccumulator;
import com.example.model.ShareVolume;
import com.example.model.StockTickerData;
import com.example.model.StockTransaction;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class StreamsSerdes {

    public static Serde<Purchase> PurchaseSerde() {
        return new PurchaseSerde();
    }

    public static Serde<PurchasePattern> PurchasePatternSerde() {
        return new PurchasePatternSerde();
    }

    public static Serde<RewardAccumulator> RewardAccumulatorSerde() {
        return new RewardAccumulatorSerde();
    }

    public static Serde<StockTickerData> StockTickerSerde() {
        return new StockTickerSerde();
    }

    public static Serde<StockTransaction> StockTransactionSerde() {
        return new StockTransactionSerde();
    }

    public static Serde<ShareVolume> ShareVolumeSerde() {
        return new ShareVolumeSerde();
    }

    public static Serde<FixedSizePriorityQueue> FixedSizePriorityQueueSerde() {
        return new FixedSizePriorityQueueSerde();
    }


    public static final class PurchaseSerde extends Serdes.WrapperSerde<Purchase> {
        public PurchaseSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Purchase.class));
        }
    }

    public static final class PurchasePatternSerde extends Serdes.WrapperSerde<PurchasePattern> {
        public PurchasePatternSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(PurchasePattern.class));
        }
    }

    public static final class RewardAccumulatorSerde extends Serdes.WrapperSerde<RewardAccumulator> {
        public RewardAccumulatorSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(RewardAccumulator.class));
        }
    }

    public static final class StockTickerSerde extends Serdes.WrapperSerde<StockTickerData> {
        public StockTickerSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTickerData.class));
        }
    }

    public static final class StockTransactionSerde extends Serdes.WrapperSerde<StockTransaction> {
        public StockTransactionSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(StockTransaction.class));
        }
    }

    public static final class ShareVolumeSerde extends Serdes.WrapperSerde<ShareVolume> {
        public ShareVolumeSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(ShareVolume.class));
        }
    }

    private static class FixedSizePriorityQueueSerde extends Serdes.WrapperSerde<FixedSizePriorityQueue> {
        public FixedSizePriorityQueueSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(FixedSizePriorityQueue.class));
        }
    }
}
