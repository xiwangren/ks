package com.kafka.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class HelloWorldConsumer {
	private static String BOOTSTAP_SERVER = "120.26.230.18:9092";
	private final static int minBatchSize = 100;

	public void HelloworldConsumerTc1(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTAP_SERVER);
		props.put("group.id", "test2");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("max.poll.records", 10);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			int i = 0;
			for (ConsumerRecord<String, String> record : records) {
				i++;
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
				buffer.add(record);
				if (buffer.size() >= minBatchSize) {
					System.out.println("!!!!!!!!!!commit offer size:" + buffer.size());
					consumer.commitAsync();
					buffer.clear();
				}
			}
			System.out.println("===================" + i);
		}
		// try {
		// while (true) {
		// ConsumerRecords<String, String> records =
		// consumer.poll(Long.MAX_VALUE);
		// for (TopicPartition partition : records.partitions()) {
		// List<ConsumerRecord<String, String>> partitionRecords =
		// records.records(partition);
		// for (ConsumerRecord<String, String> record : partitionRecords) {
		// System.out.println("topic:" + record.topic() + "\toffset:" +
		// record.offset() + "\tvalue=: "
		// + record.value());
		// }
		// long lastOffset = partitionRecords.get(partitionRecords.size() -
		// 1).offset();
		// consumer.commitSync(Collections.singletonMap(partition, new
		// OffsetAndMetadata(lastOffset + 1)));
		// }
		// }
		// } finally {
		// consumer.close();
		// }
	}

	public void HelloworldConsumerTc2(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTAP_SERVER);
		props.put("group.id", "test2");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("max.poll.records", 1000);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
		consumer.subscribe(Arrays.asList(topic));

		try {
			int i = 0;
			while (true) {
				i++;
				if (i > 5)
					break;
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				System.out.println("record count=========:" + records.count());
				// consumer.seekToBeginning(records.partitions());
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.println("topic:" + record.topic() + "\toffset:" + record.offset() + "\tvalue=: "
								+ record.value());
					}
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}
			}
		} finally {
			consumer.close();
		}
	}

	public void HelloworldConsumerTc3(String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTAP_SERVER);
		props.put("group.id", "test2");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("max.poll.records", 1000);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
		consumer.subscribe(Arrays.asList(topic));

		try {
			int i = 0;
			while (true) {
				i++;
				if (i > 5)
					break;
				ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
				System.out.println("record count=========:" + records.count());
				// consumer.seekToBeginning(records.partitions());
				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
					for (ConsumerRecord<String, String> record : partitionRecords) {
						System.out.println("topic:" + record.topic() + "\toffset:" + record.offset() + "\tvalue=: "
								+ record.value());
					}
					long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
					consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
				}
			}
		} finally {
			consumer.close();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new HelloWorldConsumer().HelloworldConsumerTc2("test");
	}

}
