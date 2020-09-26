package com.github.tausif.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyConsumer {

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		String topic = "first_topic";
		String group = "my_first_application";
		
		// Create Consumer Properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		// Value of 
		//auto.offset.reset = [earliest->Start from beginning]
		//auto.offset.reset = [latest->Reads from new message arrival]
		//auto.offset.reset = [none-It will through error if offsets is not set.]
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		// Subscribe consumer to out topic(s);
		consumer.subscribe(Arrays.asList(topic));
		
		// Poll New Data
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record: records) {
				log.info("Key : {}  Value : {} ", record.key(), record.value());
				log.info("Topic {} \n Partition : {} ", record.topic(), record.partition());
				
			}
		}

	}

}
