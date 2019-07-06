package pers.jasonLbase.common.utils;

import java.util.Locale;
import java.util.ResourceBundle;

public class I18NUtil {
	private I18NUtil(){}
	
	@SuppressWarnings("unused")
	private static Locale defaultLocale = Locale.getDefault();
	
	public static void tt(String baseName) {
		ResourceBundle.getBundle(baseName);
	}
}
