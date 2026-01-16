package com.superigno.formatting;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class App {
	
	public static void main(String[] args) throws ParseException {
		decimalFormatting();
		dateFormatting();
	}	
	
	public static void decimalFormatting() {
		/**
			# means to omit the position if no digit exists for it.
			0 means to put a 0 in the position if no digit exists for it.
		*/
		
		double d = 1234567.437;
		DecimalFormat one = new DecimalFormat("###,###,###.###");
		System.out.println(one.format(d)); // 1,234,567.437
		
		DecimalFormat two = new DecimalFormat("000,000,000.00000");
		System.out.println(two.format(d)); // 001,234,567.43700
		
		DecimalFormat three = new DecimalFormat("$#,###,###.##");
		System.out.println(three.format(d)); // $1,234,567.44
	}
	
	public static void dateFormatting() throws ParseException {
		
		DateFormat s = DateFormat.getDateInstance(DateFormat.SHORT);
		DateFormat m = DateFormat.getDateInstance(DateFormat.MEDIUM);
		DateFormat l = DateFormat.getDateInstance(DateFormat.LONG);
		DateFormat f = DateFormat.getDateInstance(DateFormat.FULL);
		DateFormat de = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.FULL); //with time
		
		
		Date d = new Date();
		
		System.out.println(s.format(d)); // ie: 7/4/15
		System.out.println(m.format(d)); // ie: Jul 4, 2015
		System.out.println(l.format(d)); // ie: July 4, 2015
		System.out.println(f.format(d)); // ie: Saturday, July 4, 2015
		System.out.println(de.format(d)); // ie: July 4, 2015 4:08:32 PM AEDT
		
		System.out.println("----------------");
		
		SimpleDateFormat f1 = new SimpleDateFormat("MM dd yyyy hh:mm:ss");
		SimpleDateFormat f2 = new SimpleDateFormat("MMMM yyyy");
		SimpleDateFormat f3 = new SimpleDateFormat("hh");
		
		Date date = f1.parse("01 26 2016 01:22:33");
		
		System.out.println(f2.format(date)); // January 2016
		System.out.println(f3.format(date)); // 01
	}

}