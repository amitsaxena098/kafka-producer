package com.kafka.producer.config;

import lombok.Getter;

@Getter
public class ProducerConfig {

    public static final String KEY_SERIALIZER = "key.serializer";
    public  static final String VALUE_SERIALIZER = "value.serializer";

}
