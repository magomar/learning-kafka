package org.learning.kafka;

import org.apache.commons.configuration2.Configuration;

public enum Config {
    APP_NAME("Twitter-Kafka-ElasticSearch"),
    HOSEBIRD_CLIENT_ID("hosebird.client_id"),
    HOSEBIRD_KEY("hosebird.key"),
    HOSEBIRD_SECRET("hosebird.secret"),
    HOSEBIRD_TOKEN("hosebird.token"),
    HOSEBIRD_TOKEN_SECRET("hosebird.token-secret"),
    HOSEBIRD_MSG_QUEUE_CAPACITY("hosebird.msg_queue_capacity", Integer.class),
    HOSEBIRD_POLLING_TIMEOUT("hosebird.polling_timeout", Long.class),
    HOSEBIRD_TERMS("hosebird.terms"),
    KAFKA_BOOTSTRAP_SERVERS("kafka.bootstrap_servers"),
    KAFKA_APPLICATION_ID("kafka.application_id"),
    KAFKA_TOPIC("kafka.topic"),
    KAFKA_GROUP_ID("kafka.group_id"),
    KAFKA_COMPRESSION_TYPE("kafka.compression_type"),
    KAFKA_LINGER_MS("kafka.linger_ms", Integer.class),
    KAFKA_BATCH_SIZE("kafka.batch_size", Integer.class),
    KAFKA_POLLING_TIMEOUT("kafka.polling_timeout", Long.class),
    KAFKA_NUM_CONSUMERS("kafka.num_consumers", Integer.class),
    BONSAI_ELASTICSEARCH_HOSTNAME("bonsai.elasticsearch.hostname"),
    BONSAI_ELASTICSEARCH_USER("bonsai.elasticsearch.username"),
    BONSAI_ELASTICSEARCH_PWD("bonsai.elasticsearch.password"),
    ELASTIC_SEARCH_INDEX("elasticsearch.index");
//    BONSAI_ELASTICSEARCH_HOST("bonsai.elasticsearch.host"),
//    BONSAI_ELASTICSEARCH_KEY("bonsai.elasticsearch.key"),
//    BONSAI_ELASTICSEARCH_SECRET("bonsai.elasticsearch.secret");

    private final String key;
    private final Class type;
    private static final Configuration config = ConfigurationLoader.load();

    Config(String key) {
        this.key = key;
        this.type = String.class;
    }

    Config(String key, Class type) {
        this.key = key;
        this.type = type;
    }

    public <T> T get() {
        return (T) config.get(type, key);
    }

}
