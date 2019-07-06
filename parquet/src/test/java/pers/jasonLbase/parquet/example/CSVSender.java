package pers.jasonLbase.parquet.example;

import java.io.BufferedOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class CSVSender {
	public static void main(String[] args) throws Exception {
		Long msgCount = 2L;
		for(int i=0; i< 20; i++) {
			String path = "file:///E:\\test\\parquet\\from\\DB_FortiSIEM_OrgEvent_SysTraffic\\tb_orgevent_systraffic\\regionoid=99\\eventdate=1525759201\\testDoc" + i + ".gz.ready";
			
			sendAsCSV(msgCount, path);
		}
	}
	
	public static void sendAsCSV(long msgCount, String path) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem hdfsFS = FileSystem.get(conf);
		FileSystem localFS = FileSystem.getLocal(conf);
		
		Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
		
//		String path = "file:///E:\\test\\parquet\\systrafficEvent-csv.gz";
//		String path = "/user/hive/warehouse/jtl.db/tb_orgevent_systraffic_csv/RegionOid=99/EventDate=1527501600/f2.gz";
		Path outputFile = new Path(path);
		FSDataOutputStream out = localFS.create(outputFile);
		int bufferSize = 1024 * 1024 * 32;
		BufferedOutputStream buffterdOut = new BufferedOutputStream(out, bufferSize);
		CompressionOutputStream cout = codec.createOutputStream(buffterdOut);

		Date time1 = new Date();
		
		long counter = 0;
		while(counter++ < msgCount) {
			cout.write(ParquetRecordGenerator.generateMsgCSV().getBytes());
		}
		
		cout.close();
		
		Date time2 = new Date();
		printTime(time1, time2, path);
	}
	
	private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static void printTime(Date time1, Date time2, String fileName) {
		System.out.println("Duration: " + TimeUnit.MINUTES.convert(time2.getTime() - time1.getTime(), TimeUnit.MILLISECONDS) + " mins (Start: " + sdf.format(time1) + ", End: " + sdf.format(time2) + ") File: " + fileName);
	}
}
