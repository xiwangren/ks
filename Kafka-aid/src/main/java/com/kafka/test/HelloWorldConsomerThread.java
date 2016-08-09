package com.kafka.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.kafka.dao.mysql.MysqlUtil;

public class HelloWorldConsomerThread implements Runnable {
	private static String BOOTSTAP_SERVER = "120.26.230.18:9092";
	private final static int minBatchSize = 100;
	private final static int TIMEOUT_CONSOMER_POLL = 100;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final Properties props = new Properties();
	private String topic = null;

	private KafkaConsumer<String, String> consumer = null;

	public HelloWorldConsomerThread(String _topic) {
		this.topic = _topic;
		props.put("bootstrap.servers", BOOTSTAP_SERVER);
		props.put("group.id", "test2");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("max.poll.records", 10);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);

	}

	public void run() {
		try {

			List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
			consumer.subscribe(Arrays.asList(this.topic));
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(TIMEOUT_CONSOMER_POLL);
				int i = 0;
				for (ConsumerRecord<String, String> record : records) {
					i++;
					System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(),
							record.value());
					buffer.add(record);
					if (buffer.size() >= minBatchSize) {
						MysqlUtil.getInstance().excuteKafkaUpdate(buffer);
						System.out.println("!!!!!!!!!!commit offer size:" + buffer.size());
						consumer.commitAsync();
						buffer.clear();
					}
				}
				if (buffer.size() > 0) {
					MysqlUtil.getInstance().excuteKafkaUpdate(buffer);
					System.out.println("!!!!!!!!!!commit offer size:" + buffer.size());
					consumer.commitAsync();
					buffer.clear();
				}
				System.out.println("===================" + i);

			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.close();
		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		for(int i=0;i < 100 ;i++){
		HelloWorldConsomerThread th = new HelloWorldConsomerThread("test");
		new Thread(th).start();
		}
	}

}
