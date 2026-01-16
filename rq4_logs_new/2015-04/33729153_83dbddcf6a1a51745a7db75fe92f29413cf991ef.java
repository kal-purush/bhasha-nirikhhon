package com.lab2.transit;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


import org.junit.Test;

import com.lab2.transit.FareCalculator;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class calculatorTest {
	private static final double DELTA = 1e-15;
	private double expected;
	private int age;
	private String time;
	private boolean isHoliday;
	
	public calculatorTest(double expected, int age, String time, boolean isHoliday){
		
		this.expected = expected;
		this.age = age;
		this.time = time;
		this.isHoliday = isHoliday;
	}
	
	@Parameters
	public static Collection<Object[]> testParams(){
		return Arrays.asList(new Object[][]{
				{0.0,4,"6:00",false},
				{2.5, 6, "20:00", false},
			
				{0.0,65,"6:00", false},//on valid
				{0.0,66,"6:00",false},//in valid
				{2.5,64,"6:00",false},//off invalid
				{0.0,88,"6:59",false}, // on valid
				{0.0,87,"9:01",false}, //on valid
				{2.5,5,"8:00",false},//off invalid
				{2.5,65,"8:00", false}, //off invalid
				{0.0,4,"7:30",true},// valid
				{0.0,66,"7:30",true},// valid
				
		});
	}
	@Test
	public void test() {
		assertEquals(expected, FareCalculator.calculateFare(age, time, isHoliday), DELTA);
		
	}

}