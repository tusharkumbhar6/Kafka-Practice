package com.tusharInc.kafkaDemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoBasicCallBack {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092"; // localhost bootstrap server
    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoBasicCallBack.class);

    public static void main(String[] args) {
        ProducerDemoBasicCallBack demoObj = new ProducerDemoBasicCallBack();
        Properties properties = demoObj.getKafkaProperties();
        demoObj.sendMessage(properties);
    }

    // Create properties
    private Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private void sendMessage(Properties properties) {
        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        // Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello World");

        // This method sends data a synchronously
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // Executes everytime record is sent or a exception is thrown
                if (e == null) {
                    LOG.info("Below Record sent successfully" +
                            "Topic : " + recordMetadata.topic() + "\n" +
                            "Partition : " + recordMetadata.partition() + "\n" +
                            "Offset : " + recordMetadata.offset() + "\n" +
                            "Timestamp : " + recordMetadata.timestamp());
                } else {
                    LOG.error("Error while producing record");
                }
            }
        });
        // flush data
        producer.flush();
        // flush data and close 
        producer.close();
    }
}
