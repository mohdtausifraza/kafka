import com.fasterxml.jackson.core.*;
import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ElasticSearchConsumer {
    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String , String> consumer = getConsumer();

        consumeAndPutToElasticSearch(client, consumer);

    }

    public static RestHighLevelClient createClient(){
        String hostName="kafka-course-5418875764.us-east-1.bonsaisearch.net";
        String userName="vkrsl2q1jr";
        String password="phr0510ze1";

        // Do not do this, if you run a local ElasticSearch
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName,password));
        RestClientBuilder builder = RestClient
                .builder(new HttpHost(hostName,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> getConsumer(){
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "twitter_tweets";
        String group = "my_elasticsearch_consumer";

        // Create Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        // Value of
        //auto.offset.reset = [earliest->Start from beginning]
        //auto.offset.reset = [latest->Reads from new message arrival]
        //auto.offset.reset = [none-It will through error if offsets is not set.]
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to out topic(s);
        consumer.subscribe(Arrays.asList(topic));

        return  consumer;
    }

    public  static void consumeAndPutToElasticSearch(RestHighLevelClient client, KafkaConsumer<String , String> consumer ) throws IOException, InterruptedException {
        // Poll New Data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            int count = records.count();
            log.info("Received {} records", count);
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record: records) {

                String jsonString = record.value();

                // Two strategies to generate ID 1. Generate with the help of kafka
                // String ID = record.topic() +"-"+record.partition()+"-"+record.offset();
                // Get the id from the tweets
                String ID = extraxtIdFromTweet(jsonString);

                //Only pass index name , passing type is deprecated.
                IndexRequest indexRequest = new IndexRequest("twitter2")
                        .source(jsonString, XContentType.JSON).id(ID);

                bulkRequest.add(indexRequest);

//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                Thread.sleep(10);
//                log.info("id :{}", indexResponse.getId());
            }
            if (count > 0) {
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                log.info("Committing Offsets");
                consumer.commitSync();
                log.info("Offsets has been Committed");
                Thread.sleep(1000);
            }

        }
//        client.close();
    }

    private static JsonParser parser = new JsonParser();
    public static String extraxtIdFromTweet(String jsonString){
        return parser.parse(jsonString)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
