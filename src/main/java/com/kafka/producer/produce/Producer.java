package com.kafka.producer.produce;

import com.kafka.producer.config.ProducerConfig;
import com.kafka.producer.interfaces.IProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
/*
Simple producer which sends messages to a topic.
 */
public class Producer implements IProducer {

    @Override
    public void produce() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("testTopic", "testValue");

        kafkaProducer.send(producerRecord);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
