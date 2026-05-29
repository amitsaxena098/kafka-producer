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
/*
This example demonstrate the behavior of StickyPartitioner which is the default
partitioner.class of kafka.
*/

@Slf4j
public class ProducerWithCallback implements IProducer {

    @Override
    public void produce() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER, StringSerializer.class.getName());
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        //To reduce batch size for demo
        kafkaProperties.put("batch.size", 300);

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties);
        //Sending 30 batches
        for(int j=0; j < 30 ; j++ ) {
            for(int i = 0; i < 10; i++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("testTopic", "hello world " + i);
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception == null) {
                            log.info("Message sent: \npartition: {}\noffset: {}", metadata.partition(), metadata.offset());
                        }
                    }
                });
            }
            //Take a pause so that kafka sends the batch
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
