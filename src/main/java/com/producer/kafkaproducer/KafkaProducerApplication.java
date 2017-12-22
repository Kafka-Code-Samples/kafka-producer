package com.producer.kafkaproducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaProducerApplication.class, args);
		String topicName = "RandomProducerTopic";
		String groupId = "RG";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupId);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		// TestCallback callback = new TestCallback();
		//Random rnd = new Random();
		for (long i = 0; i < 100; i++) {
			ProducerRecord<String, String> data = new ProducerRecord<String, String>(topicName, 0, "key-" + i,
					"message-" + i);
			RecordMetadata recordMetadata = null;
			try {
				recordMetadata = producer.send(data).get();
			} catch (Exception e) {
				System.out.println("Error while producing message to topic :" + recordMetadata);
				e.printStackTrace();
			}
			if (recordMetadata != null) {
				String message = String.format("sent message to topic : %s partition : %s  offset : %s",
						recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
				System.out.println(message);
			}
		}
		producer.close();
	}

	/*
	 * private static class TestCallback implements Callback {
	 * 
	 * @Override public void onCompletion(RecordMetadata recordMetadata,
	 * Exception e) { if (e != null) { System.out.println(
	 * "Error while producing message to topic :" + recordMetadata);
	 * e.printStackTrace(); } else { String message = String.format(
	 * "sent message to topic:%s partition:%s  offset:%s",
	 * recordMetadata.topic(), recordMetadata.partition(),
	 * recordMetadata.offset()); System.out.println(message); } } }
	 */
}
