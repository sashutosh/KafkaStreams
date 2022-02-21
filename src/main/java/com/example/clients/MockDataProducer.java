package com.example.clients;

import com.example.model.PublicTradedCompany;
import com.example.model.Purchase;
import com.example.model.StockTickerData;
import com.example.model.StockTransaction;
import com.example.util.datagen.DataGenerator;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.example.util.Topics.CLIENTS;
import static com.example.util.Topics.COMPANIES;

public class MockDataProducer {

    public static final String STOCK_TRANSACTIONS_TOPIC = "stock-transaction-stream";
    private static final Logger LOG = LoggerFactory.getLogger(MockDataProducer.class);
    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static Producer<String, String> producer;
    private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);
    private static Callback callback;
    private static volatile boolean keepRunning = true;

    public static final String STOCK_TICKER_TABLE_TOPIC = "stock-ticker-table";
    public static final String STOCK_TICKER_STREAM_TOPIC = "stock-ticker-stream";
    public static final String FINANCIAL_NEWS = "financial-news";

    public static void producePurchaseData() {
        producePurchaseData(DataGenerator.DEFAULT_NUM_PURCHASES, DataGenerator.NUM_ITERATIONS, DataGenerator.NUMBER_UNIQUE_CUSTOMERS);
    }

    public static void producePurchaseData(int numberPurchases, int numberIterations, int numberCustomers) {
        Runnable generateTask = () -> {
            init();
            int counter = 0;
            while (counter++ < numberIterations && keepRunning) {
                List<Purchase> purchases = DataGenerator.generatePurchases(numberPurchases, numberCustomers);
                List<String> jsonValues = convertToJson(purchases);
                for (String value : jsonValues) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TRANSACTIONS_TOPIC, null, value);
                    producer.send(record, callback);
                }
                LOG.info("Record batch sent");
                try {
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            LOG.info("Done generating purchase data");

        };
        executorService.submit(generateTask);
    }

    public static void produceStockTickerData(int numberCompanies, int numberIterations) {
        Runnable generateTask = () -> {
            init();
            int counter = 0;
            List<PublicTradedCompany> publicTradedCompanyList = DataGenerator.stockTicker(numberCompanies);

            while (counter++ < numberIterations && keepRunning) {
                for (PublicTradedCompany company : publicTradedCompanyList) {
                    String value = convertToJson(new StockTickerData(company.getPrice(), company.getSymbol()));

                    ProducerRecord<String, String> record = new ProducerRecord<>(STOCK_TICKER_TABLE_TOPIC, company.getSymbol(), value);
                    producer.send(record, callback);

                    record = new ProducerRecord<>(STOCK_TICKER_STREAM_TOPIC, company.getSymbol(), value);
                    producer.send(record, callback);

                    company.updateStockPrice();
                }
                LOG.info("Stock updates sent");
                try {
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            //LOG.info("Done generating StockTickerData Data");

        };
        executorService.submit(generateTask);
    }

    public static void shutdown() {
        LOG.info("Shutting down data generation");
        keepRunning = false;

        if (executorService != null) {
            executorService.shutdownNow();
            executorService = null;
        }
        if (producer != null) {
            producer.close();
            producer = null;
        }

    }

    private static <T> List<String> convertToJson(List<T> generatedDataItems) {
        List<String> jsonList = new ArrayList<>();
        for (T generatedData : generatedDataItems) {
            jsonList.add(convertToJson(generatedData));
        }
        return jsonList;
    }

    private static <T> String convertToJson(T generatedDataItem) {
        return gson.toJson(generatedDataItem);
    }

    private static void init() {

        if (producer == null) {
            LOG.info("Initializing the producer");
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "15.114.180.12:9092");
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("acks", "1");
            properties.put("retries", "3");

            producer = new KafkaProducer<>(properties);

            callback = (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                }
            };
            LOG.info("Producer initialized");
        }
    }

    public static void produceStockTransactions(int numberIterations, int numberTradedCompanies, int numberCustomers, boolean populateGlobalTables) {
        List<PublicTradedCompany> companies = getPublicTradedCompanies(numberTradedCompanies);
        List<DataGenerator.Customer> customers = getCustomers(numberCustomers);

        if (populateGlobalTables) {
            populateCompaniesGlobalKTable(companies);
            populateCustomersGlobalKTable(customers);
        }

        publishFinancialNews(companies);
        Runnable produceStockTransactionsTask = () -> {
            init();
            int counter = 0;
            while (counter++ < numberIterations && keepRunning) {
                List<StockTransaction> transactions = DataGenerator.generateStockTransactions(customers, companies, 50);
                List<String> jsonTransactions = convertToJson(transactions);
                for (String jsonTransaction : jsonTransactions) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(STOCK_TRANSACTIONS_TOPIC, null, jsonTransaction);
                    producer.send(record, callback);
                }
                LOG.info("Stock Transactions Batch Sent");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
            LOG.info("Done generating stock data");

        };
        executorService.submit(produceStockTransactionsTask);
    }

    private static List<DataGenerator.Customer> getCustomers(int numberCustomers) {
        return DataGenerator.generateCustomers(numberCustomers);
    }

    private static List<PublicTradedCompany> getPublicTradedCompanies(int numberTradedCompanies) {
        return DataGenerator.generatePublicTradedCompanies(numberTradedCompanies);
    }

    private static void populateCompaniesGlobalKTable(List<PublicTradedCompany> companies) {
        init();
        for (PublicTradedCompany company : companies) {
            ProducerRecord<String, String> record = new ProducerRecord<>(COMPANIES.topicName(), company.getSymbol(), company.getName());
            producer.send(record, callback);
        }
    }

    private static void populateCustomersGlobalKTable(List<DataGenerator.Customer> customers) {
        init();
        for (DataGenerator.Customer customer : customers) {
            String customerName = customer.getLastName() + ", " + customer.getFirstName();
            ProducerRecord<String, String> record = new ProducerRecord<>(CLIENTS.topicName(), customer.getCustomerId(), customerName);
            producer.send(record, callback);
        }
    }

    private static void publishFinancialNews(List<PublicTradedCompany> companies) {
        init();
        Set<String> industrySet = new HashSet<>();
        for (PublicTradedCompany company : companies) {
            industrySet.add(company.getIndustry());
        }
        List<String> news = DataGenerator.generateFinancialNews();
        int counter = 0;
        for (String industry : industrySet) {
            ProducerRecord<String, String> record = new ProducerRecord<>(FINANCIAL_NEWS, industry, news.get(counter++));
            producer.send(record, callback);
        }
        LOG.info("Financial news sent");
    }
}
