package com.kafka.test;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Properties;

@SuppressWarnings("Duplicates")
public class KafkaTwitterTest {

    //private ArrayList<String> al = new ArrayList<String>();
    // Instantiate Kafka Producer
    KafkaProducer<String, String> kp = KafkaTwitterProd();
//Main method
    public static void main(String[] args) {

        new KafkaTwitterTest().TwitterProducer();

    }
// Method to initiate Twitter4J producer
    public void TwitterProducer() {
        Logger logger = LoggerFactory.getLogger(KafkaTwitterTest.class.getName());
// Setup Twitter secrets 
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true).setOAuthConsumerKey("##########")
                .setOAuthConsumerSecret("############")
                .setOAuthAccessToken("######################")
                .setOAuthAccessTokenSecret("###########################");

        // simple status listner to get selective statues
        StatusListener sl = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                System.out.println(status.getUser().getName() + " : " + status.getText());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                //KafkaTwitterProd(status.getUser().getName() + " : " + status.getText());


            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };

        //add rawstream listner for pushing data in json form to kafka
        RawStreamListener rsl = new RawStreamListener() {
            @Override
            public void onMessage(String s) {

                // Alternative experiment to get data into arraylist as a buffer and then batch push
//                al.add(s);
//                //System.out.println(s);
//                logger.info("===============ArrayLength=============== "+al.size());
//                if(al.size()<100) {
//                    try {
//                        Thread.sleep(100);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }else{
//                    Iterator<String> it = al.iterator();
//                    while(it.hasNext())
//                    {
//                        KafkaTwitterProd(it.next());
//                        it.remove();
//                        System.out.println("Removed 1 tweet");
//                    }
//                }


                kp.send(new ProducerRecord<String, String>("test1", s), new Callback() {
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

            @Override
            public void onException(Exception e) {

            }
        };
        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
        twitterStream.addListener(rsl);
        twitterStream.addListener(sl);
        //twitterStream.sample();
        //System.out.println(ts);
        String[] terms = {"bjp", "india", "caa", "cab"};
        TwitterStream ts = twitterStream.filter(terms);


    }

    // Instantiation method for Kafka Producer
    @SuppressWarnings("Duplicates")
    public KafkaProducer<String, String> KafkaTwitterProd() {
        
        Properties properties = new Properties();
        String bootStrapServer = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5000");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        KafkaProducer<String, String> kp = new KafkaProducer<String, String>(properties);


        return kp;

    }
}

