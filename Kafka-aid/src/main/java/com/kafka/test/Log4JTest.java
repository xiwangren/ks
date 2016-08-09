package com.kafka.test;

import org.apache.log4j.Logger;

public class Log4JTest {
	public static void main(String[] args) {
		Logger log = null;

		try {
			// 初始化日志生成器，加载日志配置文件
			// PropertyConfigurator.configure("bin/log4j.properties");
			log = Logger.getLogger(Log4JTest.class.getName());
			log.error("main");
		} catch (Exception e) {
			log.info(e.getMessage());
			e.printStackTrace();
		}
	}
}
