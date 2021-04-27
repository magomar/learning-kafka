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
import org.aeonbits.owner.ConfigCache;
import org.learning.kafka.AppConfig;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterConsumer {
    private static final AppConfig config = ConfigCache.getOrCreate(AppConfig.class);
    private final Client twitterClient;
    private final BlockingQueue<String> msgQueue;

    public TwitterConsumer(String... terms) {
        int queueCapacity = config.twitterQueueCapacity();
        msgQueue = new LinkedBlockingQueue<>(queueCapacity);
        Hosts httpHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
//        List<Long> followings = Lists.newArrayList(1234L, 566788L);
//        List<String> terms = Lists.newArrayList("kafka");
//        twitterEndpoint.followings(followings);
        endpoint.trackTerms(Arrays.asList(terms));

        // These secrets should be read from a config file
        String consumerKey = config.twitterKey();
        String consumerSecret = config.twitterSecret();
        String token = config.twitterToken();
        String tokenSecret = config.twitterTokenSecret();
        Authentication authentication = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name(config.twitterClientId())  // optional: mainly for the logs
                .hosts(httpHosts)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);  // optional: use this if you want to process client events

        twitterClient = builder.build();
        // Attempts to establish a connection.
        twitterClient.connect();
    }

    public Client getTwitterClient() {
        return twitterClient;
    }

    public BlockingQueue<String> getMsgQueue() {
        return msgQueue;
    }
}
