package pers.jasonLbase.confluent.example;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import pers.jasonLbase.confluent.example.avro.User;

public class ConfluentKafkaConsumer {
	public static void main(String[] args) throws InterruptedException {
		boolean readAsSpecificData = true;
		
		String bootstrapServers = "192.168.1.101:9092";
		String registryUrl = "http://localhost:8081";

		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test"+System.currentTimeMillis());
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		if(readAsSpecificData) {
			props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); 
		}
		
		String topic = "confluentAvro";

		if(readAsSpecificData) {
			KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Arrays.asList(topic));
			
			int n = 1000;
			
			while(n > 0) {
				ConsumerRecords<String, User> records = consumer.poll(100);
				
				for (ConsumerRecord<String, User> record : records) {
					User r = record.value();
					System.out.print("Name: " + r.getName());
					System.out.print(" FavoriteColor: " + r.getFavoriteColor());
					System.out.println(" FavoriteNumber: " + r.getFavoriteNumber());
					
					n--;
				}
				
				TimeUnit.SECONDS.sleep(1L);
			}
			
			consumer.close();
		} else {
			KafkaConsumer<String, GenericData.Record> consumer = new KafkaConsumer<>(props);
			consumer.subscribe(Arrays.asList(topic));
			
			int n = 1000;
			
			while(n > 0) {
				ConsumerRecords<String, GenericData.Record> records = consumer.poll(100);
				
				for (ConsumerRecord<String, GenericData.Record> record : records) {
					GenericData.Record r = record.value();
					System.out.print("Name: " + r.get("name"));
					System.out.print(" FavoriteColor: " + r.get("favorite_color"));
					System.out.println(" FavoriteNumber: " + r.get("favorite_number"));
					
					n--;
				}
				
				TimeUnit.SECONDS.sleep(1L);
			}
			
			consumer.close();
		}
	}
}
