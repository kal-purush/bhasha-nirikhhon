package com.qa.web;

import java.util.Date;

public class TimeTest {
	
	public static boolean getTimeInRange(Long inputtime, Date datemin, Date datemax){
		
//		String input = inputtime;
//		System.out.println(inputtime);
	    
	    long min = datemin.getTime();
	    long max = datemax.getTime();
//	    System.out.println(max);
	    long inputno =inputtime;
//	    long inputno = Long.parseLong(input);
	    if (inputno >min && inputno < max){
	    	return true;
	    }
	    else {
	    	return false;
	    }
	}
}