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
import org.learning.kafka.Config;
import org.learning.kafka.ConfigurationLoader;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterConsumer {
    private final Client hosebirdClient;
    private final BlockingQueue<String> msgQueue;

    public TwitterConsumer(String... terms) {
        Configuration config = ConfigurationLoader.load();
        msgQueue = new LinkedBlockingQueue<>(Config.HOSEBIRD_MSG_QUEUE_CAPACITY.get());
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
//        List<String> terms = Lists.newArrayList("kafka");
//        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(Arrays.asList(terms));

        // These secrets should be read from a config file
        String consumerKey = Config.HOSEBIRD_KEY.get();
        String consumerSecret = Config.HOSEBIRD_SECRET.get();
        String token = Config.HOSEBIRD_TOKEN.get();
        String tokenSecret = Config.HOSEBIRD_TOKEN_SECRET.get();
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name(Config.HOSEBIRD_CLIENT_ID.get())  // optional: mainly for the logs
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
