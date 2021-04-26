package org.learning.kafka;

import org.aeonbits.owner.Config;

@Config.LoadPolicy(Config.LoadType.MERGE)
@Config.Sources({"system:env", "classpath:.env", "classpath:config.properties"})
public interface AppConfig extends Config {

    @Key("twitter.client_id")
    @DefaultValue("twitter-Kafka-Client")
    String twitterClientId();

    @Key("twitter.msg_queue_capacity")
    @DefaultValue("1000")
    int twitterQueueCapacity();

    @Key("twitter.polling_timeout")
    @DefaultValue("5")
    int twitterPollingTimeout();

    @Key("twitter.terms")
    @DefaultValue("cryptocurrency,blockchain,bitcoin,ethereum,litecoin,cardano")
    String twitterTerms();

    @Key("twitter.key")
    String twitterKey();

    @Key("twitter.secret")
    String twitterSecret();

    @Key("twitter.token")
    String twitterToken();

    @Key("twitter.token_secret")
    String twitterTokenSecret();

    @Key("kafka.application_id")
    @DefaultValue("twitter-to-elasticsearch")
    String applicationId();

    @Key("kafka.bootstrap_servers")
    @DefaultValue("localhost:9092")
    String bootstrapServers();

    @Key("kafka.group_id")
    @DefaultValue("twitter-group")
    String groupdId();

    @Key("kafka.topic")
    @DefaultValue("twitter-tweets")
    String topic();

    @Key("kafka.polling_timeout")
    @DefaultValue("100")
    int pollingTimeout();

    @Key("kafka.compression_type")
    @DefaultValue("snappy")
    String compressionType();

    @Key("kafka.linger_ms")
    @DefaultValue("20")
    int lingerMs();

    @Key("kafka.batch_size")
    @DefaultValue("32")
    int batchSize();

    @Key("kafka.num_consumers")
    @DefaultValue("1")
    int numConsumers();

    @Key("elasticsearch.index")
    @DefaultValue("twitter")
    String dbIndex();

    @Key("elasticsearch.hostname")
    String dbHostname();

    @Key("elasticsearch.username")
    @DefaultValue("twitter")
    String dbUsername();

    @Key("elasticsearch.password")
    @DefaultValue("twitter")
    String dbPassword();
}
