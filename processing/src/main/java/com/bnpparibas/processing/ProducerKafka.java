package com.bnpparibas.processing;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerKafka {

    public static void main(String[] args) throws Exception{

        Stream<String> filejson = Files.lines(Paths.get("/home/fouad/BigApps/kafkaBigapps/kafkaProject/processing/src/main/resources/JsonTopic.json"));

        String topicName = "test";

        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id","balami");
        props.put("key.serializer", Class.forName("org.apache.kafka.common.serialization.StringSerializer"));
        props.put("value.serializer", Class.forName("org.apache.kafka.common.serialization.StringSerializer"));

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        filejson.forEach(f->{
              ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,f);
              producer.send(record);
        });

        producer.send( new ProducerRecord<>(topicName,"k", "Message Topic 1111 partition") );
        producer.close();
    }
}
