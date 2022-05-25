package io.kafka;

import com.launchdarkly.eventsource.EventSource;
import io.handler.WikiMediaHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikiMediaProducer {

    //https://stream.wikimedia.org/v2/stream/recentchange

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "127.0.0.1:9092";
        Properties properties = initConfig(bootstrapServer);

        //kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //topic of wikimedia
        String topic = "wikimedia.recentchange";

        //event handler handling event coming from string
        EventHandler eventHandler = new WikiMediaHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        //produce for ten minutes
        TimeUnit.MINUTES.sleep(10);
    }

    private static Properties initConfig(String bootstrapServer) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //this will be default in 3, which is safe, for idempotent producer
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        prop.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prop.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");

        //compression, leave broke by default
        //snappy good for log files and json
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));  // if more than this will direct send
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");    //how long to wait (delay) on sending next batch of msg
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //there is a buffer inside producer, if buffer is full buffer.memory after time out at max.block.ms, exception would be thrown
        //if only producer is low on resources only set


        return prop;
    }


}
