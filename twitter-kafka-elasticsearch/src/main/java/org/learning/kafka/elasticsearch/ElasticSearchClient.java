package org.learning.kafka.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.JSONObject;
import org.learning.kafka.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticSearchClient {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
    private final RestHighLevelClient client;

    public static void main(String[] args) {
        ElasticSearchClient connector = new ElasticSearchClient();
        connector.insert(Config.ELASTIC_SEARCH_INDEX.get(),
                "{\"foo\":\"bar\"}");
        connector.close();

    }

    public ElasticSearchClient() {
        String hostname = Config.BONSAI_ELASTICSEARCH_HOSTNAME.get();
        String username = Config.BONSAI_ELASTICSEARCH_USER.get();
        String password = Config.BONSAI_ELASTICSEARCH_PWD.get();
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(
                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        client = new RestHighLevelClient(builder);
    }

    public RestHighLevelClient getClient() {
        return client;
    }

    private static IndexRequest createIndexRequest(String index, String jsonString) {
        IndexRequest indexRequest = new IndexRequest(index);
        JSONObject object = new JSONObject(jsonString);
        String tweetId=object.getString("id_str");
        if (null==tweetId) {
            logger.warn("Wrong data, not stored: "+jsonString);
            return null;
        }
        indexRequest.id(tweetId);
        return indexRequest;
    }

    public void insertBatch(String index, ConsumerRecords<String, String> records) {
        BulkRequest bulkRequest = new BulkRequest();
        records.forEach(record -> bulkRequest.add(createIndexRequest(index, record.value())));
        BulkResponse response = null;
        try {
            response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            logger.info("Inserted {}", records.count());
        } catch (IOException e) {
            logger.error("Error inserting data ");
            e.printStackTrace();
        }
    }

    public void insert(String index, String jsonString) {
        IndexRequest indexRequest = createIndexRequest(index, jsonString);
        indexRequest.source(jsonString, XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            logger.info("Inserted tweet with id {}", indexResponse.getId());
        } catch (IOException e) {
            logger.error("Error inserting data ");
            e.printStackTrace();
        }
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void close() {
        try {
            client.close();
        } catch (IOException e) {
            logger.error("Error closing Rest HTTP Client ");
            e.printStackTrace();
        }
    }
}
