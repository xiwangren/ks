package com.kafka.test;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerSample {
	public static void sendMessage(int start, int partition) {
		long t0 = System.currentTimeMillis();
		Map<String, Object> configs = new HashMap<String, Object>();
		configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		configs.put("bootstrap.servers", "120.26.230.18:9092");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
		for (int i = start; i < start + 10; i++) {
			producer.send(new ProducerRecord<String, String>("test", partition, "key", "value:" + i + " test data"));
		}
		producer.close();
		System.out.println("Time : " + (System.currentTimeMillis() - t0));
	}

	public static void main(String[] args) {
		new ProducerSample().sendMessage(100,0 );
	}
}
