package com.kafka.test;

import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldLogProducer {
	private static Logger LOG = LoggerFactory.getLogger(HelloWorldLogProducer.class);

	public void test_log_producer() {
		while (true) {
			LOG.info("test_log_producer : " + RandomStringUtils.random(13, "hello doctro,how are you,and you"));
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		new HelloWorldLogProducer().test_log_producer();
	}

}
