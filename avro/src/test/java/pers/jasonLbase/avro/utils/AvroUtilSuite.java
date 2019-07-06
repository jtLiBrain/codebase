package pers.jasonLbase.avro.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.junit.Test;

import pers.jasonLbase.avro.example.codec.array.User;
import pers.jasonLbase.avro.utils.AvroUtil;

public class AvroUtilSuite {
	@Test
	public void testCodecAsSpecificDatum() throws IOException {
		Schema schema = User.SCHEMA$;
		
		User u = new User();
		u.setId(1L);
		u.setName("user 1");
		
		// encode
		byte[] datum = AvroUtil.<User>encodeAsSpecificDatum(u, schema);
		
		// decode
		User decodedUser = AvroUtil.<User>decode2SpecificDatum(datum, schema);
		System.out.println("id:" + decodedUser.getId() + ", name:" + decodedUser.getName());
	}
	
	@Test
	public void testCodecAsSpecificDatum2() throws IOException {
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
		byte[] datum = AvroUtil.<List<User>>encodeAsSpecificDatum(userList, schema);
		
		// decode
		List<User> decodedUserList = AvroUtil.<List<User>>decode2SpecificDatum(datum, schema);
		for (User user : decodedUserList) {
			System.out.println("id:" + user.getId() + ", name:" + user.getName());
		}
	}
}
