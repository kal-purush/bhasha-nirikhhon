package rest;

import java.sql.Date;



import org.joda.time.DateTime;

public class DataMaper {

	public Date map(DateTime date) {
		return new Date(date.getYear(), date.getMonthOfYear(), date.getDayOfMonth());
	}

	public DateTime map(Date date) {
		return new DateTime(date.getYear(), date.getMonth(), date.getDay(), 0, 0);
	}

}