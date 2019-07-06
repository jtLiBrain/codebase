package pers.jasonLbase.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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

public class AvroUtil {
	/**
	 * 
	 * @param datum
	 * @param schema
	 * @return
	 * @throws IOException
	 */
	public static <T> byte[] encode(T datum, Schema schema) throws IOException {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
		
		DatumWriter<T> writer = new SpecificDatumWriter<T>(schema);
		
		writer.write(datum, encoder);
		encoder.flush();
		
		return outStream.toByteArray();
	}
	
	/**
	 * 
	 * @param datum
	 * @param schema
	 * @return
	 * @throws IOException
	 */
	public static byte[] encode(GenericRecord datum, Schema schema) throws IOException {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
		
		DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		
		writer.write(datum, encoder);
		encoder.flush();
		
		return outStream.toByteArray();
	}
	
	/**
	 * 
	 * @param datum
	 * @param schema
	 * @return
	 * @throws IOException
	 */
	public static <T> T decode2SpecificDatum(byte[] datum, Schema schema) throws IOException {
		InputStream inStream = new ByteArrayInputStream(datum);
		Decoder decoder = DecoderFactory.get().binaryDecoder(inStream, null);
		
		DatumReader<T> reader = new SpecificDatumReader<T>(schema);
		
		return reader.read(null, decoder);
	}
	
	/**
	 * 
	 * @param datum
	 * @param schema
	 * @return
	 * @throws IOException
	 */
	public static GenericRecord decode2GenericRecord(byte[] datum, Schema schema) throws IOException {
		InputStream inStream = new ByteArrayInputStream(datum);
		Decoder decoder = DecoderFactory.get().binaryDecoder(inStream, null);

		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);

		return datumReader.read(null, decoder);
	}
	
	/**
	 * 
	 * @param datum
	 * @param schema
	 * @return
	 * @throws IOException
	 */
	public static GenericData.Array<GenericRecord> decode2GenericRecordArray(byte[] datum, Schema schema) throws IOException {
		InputStream inStream = new ByteArrayInputStream(datum);
		Decoder decoder = DecoderFactory.get().binaryDecoder(inStream, null);

		DatumReader<GenericData.Array<GenericRecord>> datumReader = new GenericDatumReader<GenericData.Array<GenericRecord>>(schema);

		return datumReader.read(null, decoder);
	}
}
