package org.learning.kafka.twitter;

import com.twitter.hbc.core.Client;
import org.aeonbits.owner.ConfigCache;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.learning.kafka.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterKafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaProducer.class);
    private static final AppConfig config = ConfigCache.getOrCreate(AppConfig.class);
    private static final Callback sendCallback = (metadata, exception) -> {
        if (exception == null) {
            logger.info("Metadata received -> Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
        } else {
            logger.error("Error sending message to kafka");
            exception.printStackTrace();
        }
    };

    public static void main(String[] args) {
        String[] keywords = ((String) config.twitterTerms()).split(",");
        startProducer(keywords);
    }

    public static void startProducer(String[] keywords) {
        new TwitterKafkaProducer().run(keywords);
    }

    private void run(String[] keywords) {
        logger.info("Setup");

        TwitterConsumer twitterConsumer = new TwitterConsumer(keywords);
        Client client = twitterConsumer.getTwitterClient();
        BlockingQueue<String> msgQueue = twitterConsumer.getMsgQueue();
        KafkaProducer<String, String> producer = getKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            logger.info("Stopping Twitter client");
            client.stop();
            logger.info("Closing Kafka producer");
            producer.close();
            logger.info("Producer terminated");
        }
        ));

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(config.twitterPollingTimeout(), TimeUnit.SECONDS);
                if (null != msg) {
                    logger.info(msg);
                    String topic = config.topic();
                    ProducerRecord<String, String> record = new ProducerRecord<>(topic, null, msg);
                    producer.send(record, sendCallback);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
        }
        logger.info("End of application");
    }

    private static Properties getKafkaProducerConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, config.applicationId());
        // Configuration for a safer, idempotent producer in Kafka >=1.1
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        // High throughput producer (at the expense of higher latency and CPU usage)
        String compressionType = config.compressionType();
        if (null != compressionType) {
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }
        properties.put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs());
        int batchSize = config.batchSize() * 1024; // Convert to bytes
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        return properties;
    }

    public static KafkaProducer<String, String> getKafkaProducer() {
        return new KafkaProducer<>(getKafkaProducerConfig());
    }
}
