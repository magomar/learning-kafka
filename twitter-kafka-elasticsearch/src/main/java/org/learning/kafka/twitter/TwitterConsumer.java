package org.learning.kafka.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.commons.configuration2.Configuration;
import org.learning.kafka.ConfigurationLoader;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterConsumer {
    private final Client hosebirdClient;
    private final BlockingQueue<String> msgQueue;

    public TwitterConsumer(String... terms) {
        Configuration config = ConfigurationLoader.load();
        msgQueue = new LinkedBlockingQueue<>(config.getInt("hosebird.msq_queue_capacity", 1000));
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
//        List<String> terms = Lists.newArrayList("kafka");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(Arrays.asList(terms));

        // These secrets should be read from a config file
        String consumerKey = config.getString("hosebird.key");
        String consumerSecret = config.getString("hosebird.secret");
        String token = config.getString("hosebird.token");
        String tokenSecret = config.getString("hosebird.token-secret");
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Kafka-Client")  // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);  // optional: use this if you want to process client events

        hosebirdClient = builder.build();
        // Attempts to establish a connection.
        hosebirdClient.connect();
    }

    public Client getHosebirdClient() {
        return hosebirdClient;
    }

    public BlockingQueue<String> getMsgQueue() {
        return msgQueue;
    }
}
