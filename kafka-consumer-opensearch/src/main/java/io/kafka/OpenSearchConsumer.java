/**
 * consumer offset logic
 *
 * * do async, recommend manual commit offset
 *
 * -if auto commit is enabled, for every 5 second the auto commit offset method will trigger behind
 * -but if in between the service is broke, then the offset will not be re-read again
 *
 * -another one is having condition on the return of poll result
 * -when poll result is ready, do a consumer.commitAsync()
 * -but this required enable.auto.commit= false
 *
 * -enable auto commit= false, storing offsets externally (advanced!)
 * -assign partitions to ur consumer using seek() API
 * ConsumerRebalanceListener
 * -nid once processing & no way to do idempotent processing,
 * then process data + commitoffset as a single transaction
 *
 * Hearbeat
 * hearbeat.interval.ms (default 3)
 * usually set to 1/3rd of session.timeout.ms
 *
 * session.timeout.ms (default 45 seconds kafka 3.0+, before 10)
 * set lower to faster consumer rebalances
 *
 * Poll
 * max.poll.interval.ms (default 5 minutes) for big data processing
 *
 * Max Fetch Min Byte
 * Max Partition Fetch Byte
 * Fetch Max Byte
 *
 * v2,4 > can read from closest replica
 * rack.id
 * replica.selector.class = RackAwareReplicaSElector
 * **/

package io.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

    public static void main(String[] args) throws IOException {
        //1st create open search client
        RestHighLevelClient openSearchClient = createOpenSearchClient();
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

        //create index if it is not on the open search
        try (openSearchClient) {
            boolean isExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!isExists) {
                CreateIndexRequest indexRq = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(indexRq, RequestOptions.DEFAULT);
                log.info("Wikimedia Index has been created");
            } else {
                log.info("Wikimedia Index is exist");
            }

            consumer.subscribe(Collections.singletonList("wikimedia.recentchange"));

            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                BulkRequest bulkRequest = new BulkRequest();

                int recordCount = records.count();
                log.info("Received " + recordCount + " record(s)");

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        //to cater for breakdown of broker and cause alignment reread again at the same offset

                        //1st method
                        //String uniqueId = record.topic() + record.partition() + record.offSets();

                        //2nd method
                        String uniqueId = extractId(record.value());
                        //send record into open search
                        IndexRequest request = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(uniqueId);  //index is unique and opensearch can pinpoint it without inserting twice

                        bulkRequest.add(request);

                        //IndexResponse response = openSearchClient.index(request, RequestOptions.DEFAULT);
                        log.info("Insert document " + uniqueId + " into bulk");  ///wikimedia/_doc/{id}
                    } catch (Exception ex) {

                    }
                }

                if (bulkRequest.numberOfActions() > 1) {
                    BulkResponse bulk = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulk.getItems().length + " record(s) in bulk");

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                } else {

                }

                //commit offset after batch is consumer (WITH OFFSET AUTO SET TO FALSE)
                consumer.commitSync();
                log.info("OffSets is committed");
            }
        }
    }

    private static String extractId(String json) {
        //gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta").getAsJsonObject().get("id").getAsString();
    }
    public static KafkaConsumer<String,String> createKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia-group");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // disabled if u want to make use of commit offset by urself
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        KafkaConsumer<String, String > consumer = new KafkaConsumer<String, String>(props);

        return consumer;
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "https://s690fgiz6g:ozg3ari14j@kafka-courses-5964449558.ap-southeast-2.bonsaisearch.net:443";    //bonsai

        //Build URI from connection string
        RestHighLevelClient client;
        URI uri = URI.create(connString);
        String userInfo = uri.getUserInfo();

        if (userInfo == null) {
            //rest without security
            client = new RestHighLevelClient(RestClient.builder(new HttpHost(uri.getHost(), uri.getPort())));
        } else {
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0],auth[1]));

            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy()))
            );

        }

        return client;
    }
}
