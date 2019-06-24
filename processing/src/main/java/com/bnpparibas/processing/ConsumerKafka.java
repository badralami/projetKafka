package com.bnpparibas.processing;

import java.io.File;
import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerKafka {

    public static void main(String[] args) throws Exception {

        String topicName = "topic1";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "group-topic");
        props.put("client.id","balami");

        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "10000");

        props.put("key.deserializer", Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));
        props.put("value.deserializer", Class.forName("org.apache.kafka.common.serialization.StringDeserializer"));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("Subscribed to topic " + topicName);

        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.println(record.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }

}
