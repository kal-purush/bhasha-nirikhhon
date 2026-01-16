package application;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

class DateTime {
	/****Attribute****/
	private int year;
	private int month;
	private int day;
	private int hour;
	private int minute;
	
	public DateTime(int year, int month, int day, int hour, int minute) {
		this.year = year;
		this.month = month;
		this.day = day;
		this.hour = hour;
		this.minute = minute;
	}
	
	public void setDate(int year, int month, int day) {
		this.year = year;
		this.month = month;
		this.day = month;
	}
	
	public void setTime(int hour, int minute) {
		this.hour = hour;
		this.minute = minute;
	}
	
	public int getYear() {
		return year;
	}
	
	public int getMonth() {
		return month;
	}
	
	public int getDay() {
		return day;
	}
	public int getHour() {
		return hour;
	}
	
	public int getMinute() {
		return minute;
	}
	
	public long getDateTimeInMilliseconds() {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm");
		String formattedDateTime = formatDateTime();
		Date date = null;
		
		try {
			date = sdf.parse(formattedDateTime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		long dateTimeInMilliseconds = date.getTime();
		
		return dateTimeInMilliseconds;
	}
	
	public String formatDateTime() {
		String formattedDateTime = day + "/" + month + "/" + year + " " + hour + ":" + minute ;
		
		return formattedDateTime;
	}
	
}