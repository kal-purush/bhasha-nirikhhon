package com.studyguide.problems;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DateTest {

	@Test
	public void dateisbeforedate()
	{
		Date d = new Date(21,5,2007);
		Date newD = new Date(26,7,2008);
		assertEquals(true,d.isBefore(newD));
	}
	
	@Test
	public void dateisNotbeforedate()
	{
		Date d = new Date(21,5,2007);
		Date newD = new Date(26,7,2008);
		assertEquals(false,newD.isBefore(d));
	}
	
	@Test
	public void bothDateAreSame()
	{
		Date d = new Date(21,5,2007);
		assertEquals(false,d.isBefore(d));
	}
}