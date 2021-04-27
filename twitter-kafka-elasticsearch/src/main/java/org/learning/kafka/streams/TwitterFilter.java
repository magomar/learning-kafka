package org.learning.kafka.streams;

import org.aeonbits.owner.ConfigCache;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONException;
import org.json.JSONObject;
import org.learning.kafka.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TwitterFilter {
    private static final Logger logger = LoggerFactory.getLogger(TwitterFilter.class);
    private static final AppConfig config = ConfigCache.getOrCreate(AppConfig.class);

    public static void main(String[] args) {
        startStreaming();
    }

    public static void startStreaming() {
        KafkaStreams kafkaStreams = new KafkaStreams(
                getStreamsBuilder().build(),
                getKafkaConsumerConfig());
        kafkaStreams.start();
    }

    private static Properties getKafkaConsumerConfig() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.twitterFilterId());
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return properties;
    }

    private static StreamsBuilder getStreamsBuilder() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> inputTopic = streamsBuilder.stream(config.topic());
        KStream<String, String> filteredStream = inputTopic.filter(
                (key, jsonString) -> isPopularUser(jsonString));
        filteredStream.to(config.twitterFilterTopic());
        return streamsBuilder;
    }

    private static boolean isPopularUser(String jsonString) {
        try {
            JSONObject object = new JSONObject(jsonString);
            JSONObject user = object.getJSONObject("user");
            String name = user.getString("name");
            int numFollowers = user.getInt("followers_count");
            boolean isPopular = numFollowers > 1000;
            if (isPopular) System.out.println(String.format("Filter tweet by %s with %d followers.", name, numFollowers));
            return isPopular;
        } catch (JSONException e) {
            logger.warn("Skipped wrong data");
            return false;
        }
    }
}
