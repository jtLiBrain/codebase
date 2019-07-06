package pers.jasonLbase.confluent.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericData;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import pers.jasonLbase.avro.utils.AvroUtil;
import pers.jasonLbase.confluent.example.avro.User;

public class ConfluentAPI {
	public static void main(String[] args) throws IOException, RestClientException {
		String baseUrl = "http://localhost:8081";
		
		String topic = "confluentAvro";
		
		boolean readAsSpecificAvro = true;
		
		Map<String, Object> configs = new HashMap<String, Object>();
		configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrl);
		
		if(readAsSpecificAvro) {
			configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true); 
		}
		
		// 1
		KafkaAvroSerializer serializer = new KafkaAvroSerializer();
		serializer.configure(configs, true);
		
		User user = makeRecord();
		
		/*
		 * Byte Array: [0, 0, 0, 0, 21, 12, 117, 115, 101, 114, 32, 49, 2, 2, 2, 18, 114, 101, 100, 32, 99, 111, 108, 111, 114]
		 * Index     :  0  1  2  3  4  5
		 * 
		 * index 0  : Magic Byte
		 * index 1-4: Schema ID
		 * index 5  : Raw Avro Datum
		 * 
		 * see: https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
		 */
		byte[] confluentAvroBytes = serializer.serialize(topic, user);

		/*
		 * Byte Array:                 [12, 117, 115, 101, 114, 32, 49, 2, 2, 2, 18, 114, 101, 100, 32, 99, 111, 108, 111, 114]
		 */
		byte[] rawAvroBytes = AvroUtil.<User>encodeAsSpecificDatum(user, User.SCHEMA$);
		
		
		// 2
		KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
		deserializer.configure(configs, true);
		
		if(readAsSpecificAvro) {
			User u = (User) deserializer.deserialize(topic, confluentAvroBytes);
			
			System.out.println();
		} else {
			GenericData.Record r = (GenericData.Record) deserializer.deserialize(topic, confluentAvroBytes);
			
			System.out.println();
		}
	}
	
	public static User makeRecord() {
		User user = new User();
		
		user.setName("user 1");
		user.setFavoriteNumber(1);
		user.setFavoriteColor("red color");
		
		return user;
	}
}
