package com.ericsson.pc.migrationtool.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	
	
	//used to generate general_availability_date default value
	public static String getTodayAsString() {
		Date date = new Date(); 
		
		SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd");
		
		return sdf.format(date);
	}

}