package com.highill.practice.kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;



public class KafkaConsumerBase {
	
	public static <K, V> KafkaConsumer<K, V> consumer(String serverConfig, String groupId, 
			boolean autoCommit, int autoCommitIntervalMilliSecond, int sessionTimeoutMilliSecond, 
			String keyDeserializer, String valueDeserializer) {
		KafkaConsumer<K, V> kafkaConsumer = null;
		if (serverConfig != null && groupId != null && keyDeserializer != null && valueDeserializer != null) {
			Properties config = new Properties();
			config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverConfig);
			config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(autoCommit));
			config.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitIntervalMilliSecond));
			config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMilliSecond);
			// config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
			// config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
			config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);

			kafkaConsumer = new KafkaConsumer<K, V>(config);
		}

		return kafkaConsumer;
	}
	
	public static <K, V> void subscribe(String topicName, KafkaConsumer<K, V> kafkaConsumer, int timeout) {
		if(topicName != null && kafkaConsumer != null) {
			kafkaConsumer.subscribe(Collections.singleton(topicName));
			ConsumerRecords<K, V> records = kafkaConsumer.poll(timeout);
			for(ConsumerRecord<K, V> record : records) {
				System.out.println("-----received message k:" + record.key() + ",   v: " + record.value() 
						+ ",    offset: " + record.offset() + ",    partition: " + record.partition());
			}
		}
	}
	
	public static <K, V> void subscribe(String topicName, KafkaConsumer<K, V> kafkaConsumer, ConsumerRebalanceListener consumerRebanlanceListener) {
		if(topicName != null && kafkaConsumer != null && consumerRebanlanceListener != null) {
			kafkaConsumer.subscribe(Collections.singleton(topicName), consumerRebanlanceListener);
		}
	}

}