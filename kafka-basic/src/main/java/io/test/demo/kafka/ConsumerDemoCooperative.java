package io.test.demo.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        Properties prop = initConfig();
        receiveConsumer(prop);
    }

    private static Properties initConfig() {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-third-application");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // none/earliest/latest

        //
        prop.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        //prop.setProperty(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,"");

        return prop;
    }

    private static void receiveConsumer(Properties prop) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);

        //add thread
        final Thread mainThread = Thread.currentThread();

        //add shutdownhook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown .Lets exit by calling consumer.wakeup()");
                consumer.wakeup();  //this will cause poll to cause exception

                try {
                    mainThread.join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        });


        try {
            //subscribe to a topic
            consumer.subscribe(Collections.singletonList("demo_java"));

            //poll for new data
            while(true) {
                //log.info("Polling");

                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key : " + record.key() + " Value : " + record.value());
                    log.info("Partition : " + record.partition() + " Offsets : " + record.offset());
                }

            }
        } catch (WakeupException e) {
            log.info(" Wake up exception");
            //ignore as this is an expected exception
        } catch (Exception e) {
            log.error("Unexpected exception " + e.getMessage());
        } finally {
            consumer.close();   //this will also commit the offset
            log.info("Close Gracefully");
        }

    }
}
