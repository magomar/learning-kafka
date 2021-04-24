package org.learning.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoThread {
    public static void main(String[] args) {
        new ConsumerDemoThread().run();
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoThread.class);
        String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
        String GROUP_ID = "kafka-demo-thread";

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        logger.info("Creating the consumer thread");

        // crate the consumer runnable
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVERS,
                GROUP_ID, "first_topic", latch);

        // start the thread
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }
        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServers,
                                String groupId,
                                String topic,
                                CountDownLatch latch) {
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    records.forEach(record -> {
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    });
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal !");
            } finally {
                consumer.close();
                latch.countDown();
            }

        }

        public void shutdown() {
            this.consumer.wakeup();
        }
    }
}

