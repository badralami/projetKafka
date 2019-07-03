package com.bnpparibas.processing;

import com.bnpparibas.Client;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.*;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Properties;
import java.util.stream.Stream;

public class AvroProducerFile {

    public static void main(String[] args) throws Exception {


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks","1");
        props.put("retries","10");

        props.put("key.serializer", StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "AvroProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");


        Producer<String, Client> producer = new KafkaProducer<String, Client>(props);
        String topicName = "topicClient";

        Client client = Client.newBuilder()
                .setNom("Nom du client")
                .setPrenom("Prenom du client")
                .setAge(28)
                .setAdresse("25 Rue sain-maur, bagnolet, France")
                .build();

        ProducerRecord<String, Client> record = new ProducerRecord<String, Client>(topicName, client);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception){
                if(exception == null){
                    System.out.println("Le producer publie les données dans le topic");
                    System.out.println(metadata.toString());
                } else {
                    exception.printStackTrace();
                }
            }
        });

        producer.flush();
        producer.close();
    }
}
