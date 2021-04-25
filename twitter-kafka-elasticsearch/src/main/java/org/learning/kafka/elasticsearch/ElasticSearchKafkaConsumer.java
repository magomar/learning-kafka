package org.learning.kafka.elasticsearch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.learning.kafka.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ElasticSearchKafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchKafkaConsumer.class);

    public static void main(String[] args) {
        startConsumers();
    }

    public static void startConsumers() {
        new ElasticSearchKafkaConsumer().run();
    }

    private void run() {
        logger.info("Connecting to Cassandra");
        ElasticSearchClient elasticSearchClient = new ElasticSearchClient();
        logger.info("Starting Consumer Group {} with {} consumers", Config.KAFKA_GROUP_ID.get(), Config.KAFKA_NUM_CONSUMERS.get());
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        for (int i = 0; i < (int) Config.KAFKA_NUM_CONSUMERS.get(); i++) {
            String consumerId = String.format("consumer_%d", i + 1);
            Runnable consumerRunnable = new ConsumerRunnable(Config.KAFKA_TOPIC.get(),
                    consumerId,
                    Config.KAFKA_GROUP_ID.get(),
                    elasticSearchClient);
            executorService.execute(consumerRunnable);
        }
        logger.info("Shutting down Consumer Group {}");
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.info("Group execution interrupted");
            e.printStackTrace();
        } finally {
            logger.info("Finished");
        }
    }

    private static class ConsumerRunnable implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private KafkaConsumer<String, String> consumer;
        private final ElasticSearchClient elasticSearchClient;
        private final String consumerId;
        private final String consumerGroup;

        public ConsumerRunnable(String topic,
                                String consumerId,
                                String consumerGroup,
                                ElasticSearchClient elasticSearchClient) {
            this.elasticSearchClient = elasticSearchClient;
            this.consumerId = consumerId;
            this.consumerGroup = consumerGroup;
            this.consumer = new KafkaConsumer<>(getKafkaConsumerConfig());
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            while (true) {
                long millis = Config.KAFKA_POLLING_TIMEOUT.get();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(millis));
                int numRecords = records.count();
                logger.info(String.format("Received %d records", numRecords));
                String index = (String) Config.ELASTIC_SEARCH_INDEX.get();
                if (records.count() > 0) {
                    elasticSearchClient.insertBatch(index, records);
//                records.forEach(record -> elasticSearchClient.insert(index, record.value()));
                    logger.info("Committing offsets...");
                    consumer.commitSync();
                    logger.info("Offsets have been comitted");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static Properties getKafkaConsumerConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BOOTSTRAP_SERVERS.get());
//        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, MetricsCollectorConfig.KAFKA_APPLICATION_ID);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Config.KAFKA_GROUP_ID.get());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return properties;
    }
}
