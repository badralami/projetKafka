package com.bnpparibas.processing;

import java.util.*;
import java.util.Properties;
import com.bnpparibas.Client;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.*;

public class KafkaAvroConsumer{

    public static void main(String[] args) throws Exception{

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url","http://localhost:8081");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        String topic = "topicClient";
        final Consumer<String, Client> consumer = new KafkaConsumer<String, Client>(props);
        consumer.subscribe(Arrays.asList(topic));

        try{
            while (true) {
                ConsumerRecords<String, Client> records = consumer.poll(100);
                for (ConsumerRecord<String, Client> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
                }
            }
        } finally{
            consumer.close();
        }
    }

}