package pers.jasonLbase.parquet.example;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

public class ParquetRecordGenerator {
	public static MessageType getSchema() {
		MessageType schema = Types.buildMessage()
				.required(PrimitiveType.PrimitiveTypeName.INT64)	.named("id")
				.required(PrimitiveType.PrimitiveTypeName.INT32)	.named("age")
				.required(PrimitiveType.PrimitiveTypeName.BINARY)	.named("name")
				
				.named("user");
		
		return schema;
	}
	
	public static Group generateMsgGroup(MessageType schema) {
		Group group = new SimpleGroup(schema);
		
		group.add(0, 1L);
		group.add(1, 30);
		group.add(2, "user 1");
		
		return group;
	}
}
