package pers.jasonLbase.flume.sources;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class T {
	public static void main(String[] args) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		
		int length = 243;
		
		out.write(0xff & length);
		
		byte d = out.toByteArray()[0];
		
		int c = d & 0xff;
		
		System.out.print(c); 
		
	}
	
}
