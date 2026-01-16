package Util;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Formatter {
	
	public static String VEHICLE_PATTERN = "^([A-Za-z]{2})(\\d{1,2})([A-Za-z]{0,3})(\\d{1,4})$";
	public static String MOBILE_NUMBER_PATTERN = "^(\\+91)?\\d{10}$";
	public static String DATE_PATTERN = "^\\d{1,2}/\\d{1,2}/\\d{4}$";
	
	public static String getCurrencyFormat(double value){
		NumberFormat format =  NumberFormat.getInstance();
		 if (format instanceof DecimalFormat) {
		     ((DecimalFormat) format).setDecimalSeparatorAlwaysShown(true);
		     ((DecimalFormat) format).applyPattern("##,###.00");
		 }
		 return format.format(value);
	}
	
	public static String getFormattedVehicleNo(String vehicle_no){
		String v_number = null;
		Pattern p = Pattern.compile(VEHICLE_PATTERN);
		Matcher m = p.matcher(vehicle_no);
		if (m.find()) {
		    v_number = m.replaceFirst("$1 $2 $3 $4");
		}
		else{
			return vehicle_no;
		}
		
		return v_number;
	}

}