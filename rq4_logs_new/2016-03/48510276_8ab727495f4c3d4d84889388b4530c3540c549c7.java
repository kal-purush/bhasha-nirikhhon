package com.chris.oscjp.chapter5;
import java.util.*;

public class DateNumberCalls {

	public static void main(String[] args) {
		
		Locale locale = Locale.getDefault();
		System.out.println("Default Locale: "+locale);

		Date date = new Date();
		System.out.println("Current date Long = " + date.getTime());
		System.out.println("Current date = " + date.toString());
		Locale loc = new Locale("fr");
		Calendar c = Calendar.getInstance(loc);
		System.out.println("AU Time = " + c.getTime());
		Locale USloc = new Locale("US");
		Calendar uscal = Calendar.getInstance(USloc);
		System.out.println("US Time = " + uscal.getTime());

	}

}