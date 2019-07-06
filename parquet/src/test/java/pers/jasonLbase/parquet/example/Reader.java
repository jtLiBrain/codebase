package pers.jasonLbase.parquet.example;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

public class Reader {

	public static void main(String[] args) throws IOException {
		ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path("file:///F:/references/angularjs/part-00000-2775b961-8723-48b2-9bf7-d3a4d303413e-c000.gz.parquet")).build();
		 Group line=null;
	        while((line=reader.read())!=null){
	          System.out.println(line.toString());
	        }
	        System.out.println("end");
	}

}
