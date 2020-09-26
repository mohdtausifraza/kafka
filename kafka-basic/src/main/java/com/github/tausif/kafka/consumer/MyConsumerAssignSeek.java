package com.github.tausif.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MyConsumerAssignSeek {

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		String topic = "first_topic";

		// Create Consumer Properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
		
		// Create Consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		
		//Assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		long offsetToReadFrom = 15L;
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		//Seek
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		int numerOfMessageToRead=5;
		boolean keepOnReading=true;
		int numerOfMessageToReadSoFar=0;
		
		// Subscribe consumer to out topic(s);
//		consumer.subscribe(Arrays.asList(topic));
		
		// Poll New Data
		while(keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record: records) {
				numerOfMessageToReadSoFar += 1;
				log.info("Key : {}  Value : {} ", record.key(), record.value());
				log.info("Topic {} \n Partition : {} ", record.topic(), record.partition());
				if (numerOfMessageToReadSoFar >= numerOfMessageToRead ) {
					keepOnReading = false;
					break;
				}
				
			}
		}

	}

}
