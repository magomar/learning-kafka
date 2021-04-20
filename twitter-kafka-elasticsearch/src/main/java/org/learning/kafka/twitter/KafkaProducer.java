package org.learning.kafka.twitter;

import com.twitter.hbc.core.Client;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.learning.kafka.ConfigurationLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class KafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);
    private static final Configuration config = ConfigurationLoader.load();
    private static final Callback sendCallback = (metadata, exception) -> {
        if (exception == null) {
            logger.info("Metadata received -> Topic: {}, Partition: {}, Offset: {}, Timestamp: {}",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
        } else {
            logger.error("Error while producing {}", exception);
        }
    };

    public static void main(String[] args) {
        new KafkaProducer().run();
    }

    private void run() {
        logger.info("Setup");
        TwitterConsumer twitterConsumer = new TwitterConsumer(config.getString("hosebird.terms"));
        Client client = twitterConsumer.getHosebirdClient();
        BlockingQueue<String> msgQueue = twitterConsumer.getMsgQueue();
        org.apache.kafka.clients.producer.KafkaProducer producer = getKafkaProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            logger.info("Stopping Hosebird client");
            client.stop();
            logger.info("Closing Kafka producer");
            producer.close();
            logger.info("Application has exited");
        }
        ));

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            try {
                String msg = msgQueue.poll(config.getInt("hosebird.polling_timeout"), TimeUnit.SECONDS);
                if (null != msg) {
                    logger.info(msg);
                    String topic = config.getString("kafka.topic");
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
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap_servers"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, config.getString("kafka.application_id"));
        // Configuration for a safer, idempotent producer in Kafka >=1.1
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        // High throughput producer (at the expense of higher latency and CPU usage)
        String compressionType=config.getString("kafka.compression_type");
        if (null != compressionType) properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        properties.put(ProducerConfig.LINGER_MS_CONFIG,config.getString("kafka.linger_ms"));
        int batchSize=config.getInt("kafka.batch_size") * 1024; // Convert to bytes
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,Integer.valueOf(batchSize));
        return properties;
    }

    public static org.apache.kafka.clients.producer.KafkaProducer getKafkaProducer() {
        return new org.apache.kafka.clients.producer.KafkaProducer(getKafkaProducerConfig());
    }
}
