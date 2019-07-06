package pers.jasonLbase.avro.example.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import pers.jasonLbase.avro.example.codec.array.User;

public class ArrayCodec {
	private static String arraySchema = "{\"type\": \"array\", \"items\": " + User.SCHEMA$ + "}";

	public static final Schema arraySchema$ = new Schema.Parser().parse(arraySchema);

	/**
	 * 
	 * {@link org.apache.avro.generic.GenericDatumWriter#getArrayElements}
	 * 
	 * <pre>
	 * {@code
	 * protected Iterator<? extends Object> getArrayElements(Object array) {
	 *     return ((Collection) array).iterator();
	 * }
	 * </pre>
	 * 
	 * 这说明，我们可以使用任何Collection的子类作为avro array的容器
	 */
	public static byte[] encode(Collection<? extends User> users) throws IOException {
		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
		DatumWriter<Collection<? extends User>> writer = new SpecificDatumWriter<Collection<? extends User>>(
				arraySchema$);
		writer.write(users, encoder);
		encoder.flush();
		return outStream.toByteArray();
	}

	/**
	 * <p>{@link org.apache.avro.generic.GenericData#Array}</p>
	 * 
	 * <p>{@link org.apache.avro.generic.GenericDatumReader#newArray}
	 * <pre>
	 * {@code
	 * protected Object newArray(Object old, int size, Schema schema) {
	 *     if (old instanceof Collection) {
	 *         ((Collection) old).clear();
	 *         return old;
	 *     } else return new GenericData.Array(size, schema);
	 * }
	 * </pre>
	 * </p>
	 */
	public static List<User> decode2SpecificDatum(byte[] datum) throws IOException {
		InputStream inStream = new ByteArrayInputStream(datum);
		Decoder decoder = DecoderFactory.get().binaryDecoder(inStream, null);
		DatumReader<List<User>> datumReader = new SpecificDatumReader<List<User>>(arraySchema$);
		return datumReader.read(null, decoder);
	}

	/**
	 * <p>{@link org.apache.avro.generic.GenericData#Array}</p>
	 * 
	 * <p>{@link org.apache.avro.generic.GenericDatumReader#newArray}
	 * <pre>
	 * {@code
	 * protected Object newArray(Object old, int size, Schema schema) {
	 *     if (old instanceof Collection) {
	 *         ((Collection) old).clear();
	 *         return old;
	 *     } else return new GenericData.Array(size, schema);
	 * }
	 * </pre>
	 * </p>
	 */
	public static GenericData.Array<GenericRecord> decode2GenericDatum(byte[] datum) throws IOException {
		InputStream in = new ByteArrayInputStream(datum);
		Decoder decoder = DecoderFactory.get().binaryDecoder(in, null);
		DatumReader<GenericData.Array<GenericRecord>> datumReader = new GenericDatumReader<GenericData.Array<GenericRecord>>(
				arraySchema$);
		return datumReader.read(null, decoder);
	}
}
