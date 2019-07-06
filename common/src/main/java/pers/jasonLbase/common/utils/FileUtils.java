package pers.jasonLbase.common.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@SuppressWarnings("restriction")
public class FileUtils {
	private FileUtils() {}
	
	public final static String SEPARATOR_LINE;
	
	static {
		SEPARATOR_LINE = java.security.AccessController.doPrivileged(
			new sun.security.action.GetPropertyAction("line.separator")
		);
	}
	
	public static String read(InputStream in) throws IOException {
		return read(in, true);
	}
	
	public static String read(InputStream in, boolean withLineDelimiter) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		try {
			StringBuilder sb = new StringBuilder();
			
			String l;
			while ((l = reader.readLine()) != null) {
				sb.append(l);
				
				if(withLineDelimiter)
					sb.append(SEPARATOR_LINE);
			}
			
			return sb.toString();
		} finally {
			reader.close();
		}
	}
}
