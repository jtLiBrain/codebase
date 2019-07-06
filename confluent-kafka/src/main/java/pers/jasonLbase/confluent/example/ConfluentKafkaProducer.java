package pers.jasonLbase.confluent.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import pers.jasonLbase.confluent.example.avro.User;

public class ConfluentKafkaProducer {
	public static void main(String[] args) {
		String bootstrapServers = "192.168.1.101:9092";
		String registryUrl = "http://localhost:8081";

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);

		String topic = "confluentAvro";
		String key = null;

		KafkaProducer<String, User> producer = new KafkaProducer<>(props);

		User user = new User();
		user.setName("user 1");
		user.setFavoriteNumber(1);
		user.setFavoriteColor("red color");

		ProducerRecord<String, User> record = new ProducerRecord<String, User>(topic, key, user);
		producer.send(record);

		producer.close();
	}
}
