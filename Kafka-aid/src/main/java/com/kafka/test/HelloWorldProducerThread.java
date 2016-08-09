package com.kafka.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class HelloWorldProducerThread implements Runnable {
	private static String BOOTSTAP_SERVER = "120.26.230.18:9092";
	private static int BATCH_SIZE = 16384;
	private static int BUFFER_MEMORY = 33554432;
	private static int LINGER_MS = 0;
	private String topic = null;
	private String threadName = null;

	public HelloWorldProducerThread(String _threadName, String _topic) {
		this.topic = _topic;
		this.threadName = _threadName;
	}

	public void run() {
		myCallback mc = new myCallback();
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTAP_SERVER);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", BATCH_SIZE);
		props.put("linger.ms", LINGER_MS);
		props.put("buffer.memory", BUFFER_MEMORY);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(props);
		Future<RecordMetadata> rm = null;
		for (int i = 0; i < 10000000; i++) {
			rm = producer.send(new ProducerRecord<String, String>(this.topic, "mykey\t" + Integer.toString(i),
					this.threadName + "myvalue\t" + Integer.toString(i)), null);
			try {
				RecordMetadata rmd = rm.get();
				System.out.println(rmd.topic());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		producer.flush();
		producer.close();

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for (int i = 0; i < 100; i++) {
			HelloWorldProducerThread hproduct = new HelloWorldProducerThread("Thread" + i, "test");
			Thread th = new Thread(hproduct);
			th.start();
		}
	}

}
