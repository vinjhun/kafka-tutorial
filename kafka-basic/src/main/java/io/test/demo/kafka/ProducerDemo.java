package io.test.demo.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    //kafka-topics --bootstrap-server localhost:9092 --create --topic demo_java --partitions 3 --replication-factor 1
    //
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello World!");

        Properties prop = initConfig();
        //sendHello(prop);
        sendCallback(prop);
    }

    private static Properties initConfig() {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //kafka > 3.0
        //prop.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        //prop.setProperty("enable.idempotent", "true");

        //kafka 2.8 =<
        //prop.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        //prop.setProperty("enable.idempotent", "false");
        return prop;
    }

    private static void sendHello(Properties properties) {
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create data
        ProducerRecord<String, String > producerRecord =
                new ProducerRecord<>("demo_java", "hello_word");

        //send data - asynchronous
        producer.send(producerRecord);

        //flush and close producer
        //flush data - synchronous
        producer.flush();

        //close
        producer.close();
    }

    private static void sendCallback(Properties properties) {
        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        //send data - asynchronous
        for (int i = 0; i < 10; i++) {

            /*
            * ProducerRecord<String, String > producerRecord =
                    new ProducerRecord<>("demo_java", call_back" + i);
            * */

            //create data in key value
            ProducerRecord<String, String > producerRecord =
                    new ProducerRecord<>("demo_java", "id_" + i, "call_back" + i);

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    if (e == null) {
                        log.info("Received new metadata / \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Key: " + producerRecord.key() + "\n" +
                                "Offset: " + metadata.hasOffset() + "\n" +
                                "Timestamp:" + metadata.timestamp() + "\n");
                    } else {
                        log.error(e.getMessage());
                    }
                }
            });

            //sticky partitioner will send msg in batches if the msg is send fast enough

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        //flush and close producer
        //flush data - synchronous
        producer.flush();

        //close
        producer.close();
    }
}
