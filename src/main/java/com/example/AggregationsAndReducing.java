package com.example;

import com.example.clients.MockDataProducer;
import com.example.collectors.FixedSizePriorityQueue;
import com.example.model.ShareVolume;
import com.example.model.StockTransaction;
import com.example.serdes.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Properties;

import static com.example.clients.MockDataProducer.STOCK_TRANSACTIONS_TOPIC;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

public class AggregationsAndReducing {
    public static final Logger LOG = LoggerFactory.getLogger(AggregationsAndReducing.class);

    public static void main(String[] args) {
        StreamsConfig streamsConfig = new StreamsConfig(getProperty());
        Serde<String> stringSerde = Serdes.String();
        Serde<StockTransaction> stockTransactionSerde = StreamsSerdes.StockTransactionSerde();
        Serde<ShareVolume> shareVolumeSerde = StreamsSerdes.ShareVolumeSerde();
        Serde<FixedSizePriorityQueue> fixedSizePriorityQueueSerde = StreamsSerdes.FixedSizePriorityQueueSerde();
        Comparator<ShareVolume> comparator = (sv1, sv2) -> sv2.getShares() - sv1.getShares();
        FixedSizePriorityQueue<ShareVolume> fixedQueue = new FixedSizePriorityQueue<>(comparator, 5);
        NumberFormat numberFormat = NumberFormat.getInstance();

        ValueMapper<FixedSizePriorityQueue, String> valueMapper = fpq -> {
            StringBuilder builder = new StringBuilder();
            Iterator<ShareVolume> iterator = fpq.iterator();
            int counter = 1;
            while (iterator.hasNext()) {
                ShareVolume stockVolume = iterator.next();
                if (stockVolume != null) {
                    builder.append(counter++).append(")").append(stockVolume.getSymbol())
                            .append(":").append(numberFormat.format(stockVolume.getShares())).append(" ");
                }
            }
            return builder.toString();
        };

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KTable<String, ShareVolume> shareVolumeStream = streamsBuilder.stream(STOCK_TRANSACTIONS_TOPIC, Consumed.with(stringSerde, stockTransactionSerde).withOffsetResetPolicy(EARLIEST))
                .mapValues(st -> ShareVolume.newBuilder(st).build())
                .groupBy((key, value) -> value.getSymbol(), Grouped.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);

        //shareVolumeStream.toStream().to("share-volume", Produced.with(stringSerde,shareVolumeSerde));
        //shareVolumeStream.toStream().print(Printed.<String,ShareVolume>toSysOut().withLabel("share-volume"));

        shareVolumeStream.groupBy((key, value) -> KeyValue.pair(value.getSymbol(), value), Grouped.with(stringSerde, shareVolumeSerde))
                .aggregate(() -> fixedQueue,
                        (k, v, aggr) -> aggr.add(v),
                        (k, v, aggr) -> aggr.remove(v),
                        Materialized.with(stringSerde, fixedSizePriorityQueueSerde))
                .mapValues(valueMapper)
                .toStream().peek((k, v) -> LOG.info("Stock volume by industry {} {}", k, v))
                .to("stock-volume-by-company", Produced.with(stringSerde, stringSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsConfig);
        MockDataProducer.produceStockTransactions(15, 50, 25, false);
        LOG.info("First Reduction and Aggregation Example Application Started");
        kafkaStreams.start();
        try {
            Thread.sleep(65000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOG.info("Shutting down the Reduction and Aggregation Example Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }

    private static Properties getProperty() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KTable-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KTable-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "15.114.180.12:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }


}
