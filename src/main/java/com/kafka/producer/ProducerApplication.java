package com.kafka.producer;

import com.kafka.producer.interfaces.IProducer;
import com.kafka.producer.produce.ProducerWithCallback;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ProducerApplication  implements CommandLineRunner {

    public static void main(String[] args) {
		SpringApplication.run(ProducerApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
        IProducer producer = new ProducerWithCallback();
		producer.produce();
	}
}
