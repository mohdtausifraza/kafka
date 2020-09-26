package com.github.tausif.kafka.producer;


import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public class MyProducer {

	public static void main(String[] args) {
		String bootstrapServer = "127.0.0.1:9092";
		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		
		
		for (int i = 0; i < 10; i++) {
			// Create Producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello Wolrd : "+i);
			
			// Send the data
			producer.send(record, new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// Executes every time a record is successfully send or an exception is thrown.
					if (exception ==null) {
						log.info("Received new Metadata. \n"
								+ "Topic : {} \n"
								+ "Partition : {} \n"
								+ "Offset : {} \n"
								+ "Timestamp : {} \n",metadata.topic() ,  metadata.partition(), metadata.offset() , metadata.timestamp() 
								);
					
					}
					
				}
			});	
		}
					
		// Flush the Data
		producer.flush();
		
		// OR Flush and Close
		producer.close();
		
	}
}
