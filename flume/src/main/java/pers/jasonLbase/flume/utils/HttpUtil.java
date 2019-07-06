package pers.jasonLbase.flume.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

public class HttpUtil {
	public static List<ByteBuffer> readBuffers(ByteBuf in) throws IOException {
		List<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
		
		while (in.readableBytes() > 4) {
			int length = (in.readByte()          << 24) + 
						 ((in.readByte() & 0xFF) << 16) + 
						 ((in.readByte() & 0xFF) << 8) + 
						 (in.readByte()  & 0xFF);
			
			if (length == 0) { // end of buffers
				return buffers;
			}
			
			/*ByteBuffer buffer = ByteBuffer.allocate(length);
			int p = buffer.position();
			in.readBytes(buffer.array());
			buffer.position(p + length);
			buffer.flip();
			buffers.add(buffer);*/
			
			ByteBuffer buffer = ByteBuffer.allocate(length);
			while (buffer.hasRemaining()) {
				int p = buffer.position();
				int remaining = buffer.remaining();
				in.readBytes(buffer.array(), p, remaining);
				buffer.position(p + remaining);
			}
			buffer.flip();
			buffers.add(buffer);
		}

		return buffers;
	}
}
