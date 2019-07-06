package pers.jasonLbase.avro.example.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import pers.jasonLbase.avro.example.codec.simpleRecord.User;

public class RecordCodec {
	public static byte[] encodeWithSpecificDatum(User user) throws IOException {
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(baOutStream, null);
		
		/*
		 * 参看 ：SpecificData.getSchema(), SpecificData.createSchema()
		 * SpecificDatumWriter构造参数中的 Class 用于解析schema，并且使用该类中的 SCHEMA$ 字段作为schema
		 * 
		 * 我们来理解一下SpecificDatumWriter这个类的构造方法：
		 * 1. 它的类型参数是来说明它每次执行 write 时，datum的数据类型
		 * 2. 它的构造参数是用来提供schema的查找方案
		 */
		DatumWriter<User> writer = new SpecificDatumWriter<User>(User.class);
		//or : DatumWriter<User> writer = new SpecificDatumWriter<User>(User.SCHEMA$);
		
		/*
		 * 这个方法在写入的时候调用了GenericDatumWriter.writeRecord()，这个方法会调用我们Email.get(int)，
		 * 所以，这要求我们的Email需要实现IndexedRecord接口，而SpecificRecord接口继承了IndexedRecord，
		 * 因此，我们的Email的也可以直接实现SpecificRecord接口
		 */
		writer.write(user, encoder);
		encoder.flush();
		
		return baOutStream.toByteArray();
	}

	public static byte[] encodeWithGenericDatum(GenericRecord record) throws IOException {
		ByteArrayOutputStream baOutStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(baOutStream, null);
		
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(User.SCHEMA$);
		
		writer.write(record, encoder);
		encoder.flush();
		
		return baOutStream.toByteArray();
	}
	
	public static User decode2SpecificDatum(byte[] datum) throws IOException {
		InputStream inStream = new ByteArrayInputStream(datum);
		Decoder decoder = DecoderFactory.get().binaryDecoder(inStream, null);

		DatumReader<User> datumReader = new SpecificDatumReader<User>(User.class);
		// or : DatumReader<User> datumReader = new SpecificDatumReader<User>(User.SCHEMA$);
		
		return datumReader.read(null, decoder);
	}
	
	public static GenericRecord decode2GenericDatum(byte[] datum) throws IOException {
		InputStream inStream = new ByteArrayInputStream(datum);
		Decoder decoder = DecoderFactory.get().binaryDecoder(inStream, null);
		
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(User.SCHEMA$);
		
		return datumReader.read(null, decoder);
	}
}
