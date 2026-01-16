package test;

import java.util.Calendar;
import java.util.Date;

public class Main {

	
public static void main(String[] args) {

	//You may test that your code works find here
	//Please check that your code works and has no 
	//compilation problems before to submit
	System.out.println("MAIN CLASS");
	
	Date date=Calendar.getInstance().getTime();
	System.out.println(""+date.getHours());
	
	Student std=new Student(1, "venkat", date, 73.4d);
	
	
	
}
}