package com.github.tausif.kafka.twitter.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
public class TwitterProducer {

	public static void main(String[] args) {
		new TwitterProducer().run();

	}

	public void run() {
		/**
		 * Set up your blocking queues: Be sure to size these properly based on expected
		 * TPS of your stream
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		// Create a Twitter client
		Client client = createTwitterClient(msgQueue);
		// Attempts to stablish a connection
		client.connect();
		
		// Create a kafka producer
		KafkaProducer<String, String> producer = createKafkaProducer();
		
		//Add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread( () -> {
			log.info("Stopping Application");
			log.info("Shutting down client for twitter..");
			client.stop();
			log.info("Closing Producer...");
			producer.close();
			log.info("Done...");
			
		}));
		
		// Loop to send tweets to kafka
		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg =null;
			try {
				msg = msgQueue.poll(5 , TimeUnit.SECONDS);
			} catch (Exception e) {
				client.stop();
			}
			if (msg != null) {
				log.info(msg);
				producer.send(new ProducerRecord<>("twitter_tweets", null ,msg), new Callback() {
					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if (exception != null) {
							log.info("Something Bad Happened", exception);
						}else {
							log.info("Produced to : topic : {} partition : {} " ,metadata.topic(),metadata.partition());
						}
						
					}
				});
				
			} 
		}
		log.info("End of Application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		String consumerKey = "FVctDL9xpQZ0tRh0oMKCadPc8";
		String consumerSecret = "HDDdncJByIufw9Xy9xCRo2r7yjUOCuWfwNIbfZmGzyiWKbJRhm";
		String token = "2349810414-rjNtejrcE5fJwdnmvCWaltJP5o0Tke4Fhe3mnYv";
		String secret = "LiDM3KbNpZ260Uw3d0MUwLbeqHAbHH8m13GHTAW7AtPYM";
		
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up some followings and track terms
		List<String> terms = Lists.newArrayList("usa","india","politics");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Client Builder
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
	
	public KafkaProducer<String, String> createKafkaProducer() {
		String bootstrapServer = "127.0.0.1:9092";
		// Create Producer Properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create Safe Producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG , "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION , "5"); 
		
		// High throughput producer (At the expense of little latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		
		// Create the Producer
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		return producer;
	}

}
