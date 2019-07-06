package pers.jasonLbase.parquet.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter.Builder;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;

public class ParquetGroupBuilder extends Builder<Group, ParquetGroupBuilder> {
	private MessageType schema;
	
	public ParquetGroupBuilder(MessageType schema, String file) {
		this(file);
		this.schema = schema;
	}
	
	public ParquetGroupBuilder(MessageType schema, Path file) {
		this(file);
		this.schema = schema;
	}
	
	public ParquetGroupBuilder(String file) {
		this(new Path(file));
	}
	
	public ParquetGroupBuilder(Path file) {
		super(file);
	}
	
	@Override
	public ParquetGroupBuilder self() {
		return this;
	}

	@Override
	protected WriteSupport<Group> getWriteSupport(Configuration conf) {
		if(schema != null) {
			GroupWriteSupport.setSchema(schema, conf);
		}
		
		return new GroupWriteSupport();
	}
}
