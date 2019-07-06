package pers.jasonLbase.parquet.example;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import pers.jasonLbase.parquet.utils.ParquetGroupBuilder;


public class Sender {
	public static final int K_BYTES = 1024;
	public static final int M_BYTES = 1024 * 1024;
	public static final int G_BYTES = 1024 * 1024 * 1024;
	
	public static void main(String[] args) throws IOException {
		long perFile = 5000000L;
		long fileCount = 1L;
		
		String compressionCodec = "gz";

		CompressionCodecName codecName = null;
		MessageType schema = ParquetRecordGenerator.getSchema();
		
//		long total = 89L;
//		long fileCount = total/perFile + (total%perFile>0 ? 1 : 0);
		
		// a file path conforming to HDFS file name Specification
		String filePattern = "file:///E:\\test\\parquet\\testDoc{0}{1}.parquet";
//		String filePattern = "/user/hive/warehouse/jtl.db/tb_orgevent_systraffic_parquet_timestamp/RegionOid=99/EventDate=1527501600/f4{0}{1}.parquet";
		if(StringUtils.isBlank(compressionCodec)) {
			filePattern = filePattern.replaceAll("\\{1\\}", "");
			codecName = CompressionCodecName.UNCOMPRESSED;
		} else if (compressionCodec.equals("snappy")){
			filePattern = filePattern.replaceAll("\\{1\\}", ".snappy");
			codecName = CompressionCodecName.SNAPPY;
		} else if (compressionCodec.equals("gz")){
			filePattern = filePattern.replaceAll("\\{1\\}", ".gz");
			codecName = CompressionCodecName.GZIP;
		} else if (compressionCodec.equals("lzo")){
			filePattern = filePattern.replaceAll("\\{1\\}", ".lzo");
			codecName = CompressionCodecName.LZO;
		}
		
		for(long idx=0; idx<fileCount; idx++) {
			String fileToBeWritten = filePattern.replaceAll("\\{0\\}", ""+idx);
			
			sendMsgToFile(schema, fileToBeWritten, perFile, codecName);
		}
	}

	private static void sendMsgToFile(MessageType schema, String toFile, long msgCount, CompressionCodecName codecName) throws IOException {
		ParquetGroupBuilder pgb = new ParquetGroupBuilder(schema, toFile);
		
		pgb.withWriteMode(ParquetFileWriter.Mode.CREATE); // don't have default value
		pgb.withCompressionCodec(codecName); // default uncompressed
		pgb.withRowGroupSize(M_BYTES*128);
		pgb.withPageSize(M_BYTES);
		pgb.withDictionaryPageSize(M_BYTES);
		pgb.withDictionaryEncoding(ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED);
		pgb.withValidation(false);
		pgb.withWriterVersion(WriterVersion.PARQUET_1_0);
		pgb.withMaxPaddingSize(0);
		
		ParquetWriter<Group> writer = pgb.build();
		
		Date time1 = new Date();
		
		long counter = 0;
		while(counter++ < msgCount) {
			writer.write(ParquetRecordGenerator.generateMsgGroup(schema));
		}
		
		writer.close();
		
		Date time2 = new Date();
		printTime(time1, time2, toFile);
	}

	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static void printTime(Date time1, Date time2, String fileName) {
		System.out.println("Duration: " + TimeUnit.MINUTES.convert(time2.getTime() - time1.getTime(), TimeUnit.MILLISECONDS) + " mins (Start: " + sdf.format(time1) + ", End: " + sdf.format(time2) + ") File: " + fileName);
	}
}
