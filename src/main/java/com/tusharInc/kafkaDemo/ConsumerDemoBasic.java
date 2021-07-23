package com.tusharInc.kafkaDemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoBasic {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092"; // localhost bootstrap server
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoBasic.class);
    private static final String GROUP_ID = "my-new-java-application";
    private static final String TOPIC = "first_topic";

    // Create consumer config
    private Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        /* Supported values for AUTO_OFFSET_RESET_CONFIG are
           earliest- from begingin of topic
           latest- read only from latest
           none- throw error when offset is not saved
        */
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }

    private void readMessage(Properties properties) {
        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // Subscribe consumer to topic(s)
        consumer.subscribe(Arrays.asList(TOPIC));

        // poll for new data
        while(true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                LOG.info("Key : " + record.key() + ", Value : " + record.value() +
                        ", Partition : " + record.partition() + ", Offset : " + record.offset());
            }
        }
    }


    public static void main(String[] args) {
        ConsumerDemoBasic demoObj = new ConsumerDemoBasic();
        Properties properties = demoObj.getKafkaProperties();
        demoObj.readMessage(properties);
    }
}
