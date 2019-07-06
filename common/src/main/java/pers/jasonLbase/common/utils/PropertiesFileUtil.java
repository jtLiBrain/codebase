package pers.jasonLbase.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesFileUtil {
	public static Properties read(InputStream inputStream) {
		Properties depts = new Properties();
		try {
			depts.load(inputStream);
		} catch (IOException e) {
			return null;
		} finally {
			try {
				if (inputStream != null) {
					inputStream.close();
				}
			} catch (IOException e) {
			}
		}
		return depts;
	}
}
