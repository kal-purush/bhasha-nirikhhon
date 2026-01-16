package com.rays.javabasic;

import java.time.LocalDate;
import java.time.Period;

public class AgeCount {
	
	public static void main(String[] args) {
		
		LocalDate date = LocalDate.now();
		
		LocalDate empbirth = LocalDate.of(1997, 8, 05);
		
		Period p = Period.between(empbirth, date);
		
		System.out.println("age : " + p.getYears());
	}

}