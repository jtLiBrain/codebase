package pers.jasonLbase.avro.example.codec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import pers.jasonLbase.avro.example.codec.ArrayCodec;
import pers.jasonLbase.avro.example.codec.array.User;


public class ArrayCodecSuite {
	List<User> userList = new ArrayList<User>();
	
	@Before
	public void prepared() {
		User user1 = new User();
		user1.setId(1L);
		user1.setName("User 1");
		
		User user2 = new User();
		user2.setId(2L);
		user2.setName("User 2");
		
		userList.add(user1);
		userList.add(user2);
	}
	
	@Test
	public void decode2SpecificDatum() throws IOException {
		byte[] encodeDatum = ArrayCodec.encode(userList);
		
		List<User> result = ArrayCodec.decode2SpecificDatum(encodeDatum);
		
		for (User user : result) {
			System.out.println("id:" + user.getId() + ", name:" + user.getName());
		}
	}
	
	@Test
	public void decode2GenericDatum() throws IOException {
		byte[] encodeDatum = ArrayCodec.encode(userList);
		
		GenericData.Array<GenericRecord> result = ArrayCodec.decode2GenericDatum(encodeDatum);
		
		for (GenericRecord genericRecord : result) {
			System.out.println("id:" + genericRecord.get("id") + ", name:" + genericRecord.get("name"));
		}
	}
}
