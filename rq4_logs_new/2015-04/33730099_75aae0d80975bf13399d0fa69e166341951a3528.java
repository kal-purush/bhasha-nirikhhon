package com.lab2.transit;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.lab2.transit.FareCalculator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class FareCalculatorTest {
	
	private double expected;
	private int age;
	private String time;
	private boolean isHoliday;
	private	static	final	
	double	DELTA =	1e-15;
	
	public FareCalculatorTest(double expected, int age, String time, boolean isHoliday)
	{
		this.expected = expected; 
		this.age = age;
		this.time = time;
		this.isHoliday = isHoliday;
	}
	
	@Parameters
	public static Collection<Object[]> testParams()
	{
	return Arrays.asList(new Object[][]{
		{0.0,4,"6:00", false},
		{0.0,5,"6:00", false},
		{2.5,6,"6:00", false},
		{2.5,64,"6:00", false},
		{0.0,65,"6:00", false},
		{0.0,66,"6:00", false},
		{0.0,67,"6:59", false},
		{2.5,67,"7:00", false},
		{2.5,67,"7:01", false},
		{2.5,67,"8:59", false},
		{2.5,67,"9:00", false},
		{0.0,67,"9:01", false},
		{2.5,30,"7:30", true},
		{0.0,4,"5:30", true},
		{0.0,67,"5:30", true},
		{0.0,67,"8:30", true}
		
			});
	}
	
	@Test
	public void calculatorFareTest()
	{
		assertEquals(expected, FareCalculator.calculateFare(age, time, isHoliday), DELTA);
	}



}