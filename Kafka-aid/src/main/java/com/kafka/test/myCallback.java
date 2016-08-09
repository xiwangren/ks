package com.kafka.test;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class myCallback implements Callback {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		System.out.println("onCompletion done!");
	}

}
