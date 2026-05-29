package com.kafka.producer.produce;

import com.kafka.producer.config.ProducerConfig;
import com.kafka.producer.interfaces.IProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerWithKey implements IProducer {
    @Override
    public void produce() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties);
        String topic = "testTopic";
        for(int j = 0; j < 2; j++) {
            for(int i = 0; i < 10; i++) {
                String key = "key-" + i;
                String value = "hello world " + i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception == null) {
                            log.info("Message sent:\n key: {}, partition: {}, value: {}", key, metadata.partition(), value);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
