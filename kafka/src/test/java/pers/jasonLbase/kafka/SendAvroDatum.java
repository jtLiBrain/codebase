package pers.jasonLbase.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;

import pers.jasonLbase.avro.example.codec.array.User;
import pers.jasonLbase.avro.utils.AvroUtil;

public class SendAvroDatum {

	public static void main(String[] args) throws IOException {
		KafkaProducerClient<String, byte[]> client = new KafkaProducerClient<String, byte[]>();
		
		client.setBootstrapServers("localhost:9092");
		client.init();
		
		// 
		String topic = "topicStreaming";
		String key = null;
		byte[] value = makeAvroDatum();
		
		client.send(topic, key, value);
		
		client.close();
	}
	
	public static byte[] makeAvroDatum() throws IOException {
		User user1 = new User();
		user1.setId(1L);
		user1.setName("User 1");
		
		Schema schema = User.SCHEMA$;
		
		// encode
		return AvroUtil.<User>encodeAsSpecificDatum(user1, schema);
	}
	
	public static byte[] makeAvroDatumList() throws IOException {
		List<User> userList = new ArrayList<User>();
		
		User user1 = new User();
		user1.setId(1L);
		user1.setName("User 1");
		
		User user2 = new User();
		user2.setId(2L);
		user2.setName("User 2");
		
		userList.add(user1);
		userList.add(user2);
		
		String arraySchema =
				"{" +
					"\"type\": \"array\"," +
					"\"items\": " + User.SCHEMA$ +
				"}";
		
		Schema schema = new Schema.Parser().parse(arraySchema);
		
		// encode
		return AvroUtil.<List<User>>encodeAsSpecificDatum(userList, schema);
	}

}
