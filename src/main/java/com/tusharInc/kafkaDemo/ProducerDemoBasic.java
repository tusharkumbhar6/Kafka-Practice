package com.tusharInc.kafkaDemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemoBasic {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092"; // localhost bootstrap server

    public static void main(String[] args) {
        ProducerDemoBasic demoObj = new ProducerDemoBasic();
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
        producer.send(record);
        // flush data
        producer.flush();
        // flush data and close 
        producer.close();
    }
}
