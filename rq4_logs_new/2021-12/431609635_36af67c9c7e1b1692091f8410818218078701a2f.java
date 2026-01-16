package models.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateFormatter {
	public static String getFormattedDate(Date date) {
		String formattedDate = new SimpleDateFormat("EEEE d MMMM Y").format(date);
		return formattedDate;
	}
}