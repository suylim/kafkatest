package com.kafka.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.util.Properties;

public class ProducerDemo {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
//        System.out.println("hello");
        Properties properties = new Properties();
        String bootStrapServer = "localhost:9092";
//        properties.setProperty("bootstrap.servers",bootStrapServer);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.Serializer",StringSerializer.class.getName());
        // New Way
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> kp = new KafkaProducer<String, String>(properties)) {


            ProducerRecord<String, String> pr = new ProducerRecord<String, String>("test1", "hello world");
            for (int i =0 ; i <10 ; i++) {
                kp.send(pr, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                        if (e == null) {

                            logger.info("Topic name :" + recordMetadata.topic());
                            logger.info("Offset : " + recordMetadata.offset());
                            logger.info("Timestamp" + recordMetadata.timestamp());

                        } else {
                            logger.error(e.toString());
                        }

                    }
                });
            }
            

        }
    }
}
