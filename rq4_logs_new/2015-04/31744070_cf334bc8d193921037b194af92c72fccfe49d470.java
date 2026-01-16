package calendar;
import java.util.Scanner;
import java.util.TimeZone;

import static org.junit.Assert.*;

import org.junit.Test;

public class CalendarDriverTest {

	@Test
	public final void testMain() {
		CalendarDriver.main(null);
	}

	@Test
	public final void testGetFileName() {
		Scanner input1 = new Scanner(System.in);
		assertEquals("test.ics", CalendarDriver.getFileName(input1));
	}
	
	@Test 
	public final void testNumberInputGetFileName(){
		Scanner input2 = new Scanner(System.in);
		assertEquals("123.ics", CalendarDriver.getFileName(input2));
	}
	
	@Test 
	public final void testSpecialCharInputGetFileName(){
		Scanner input3 = new Scanner(System.in);
		assertEquals("#@%.ics", CalendarDriver.getFileName(input3));
	}
	
	@Test
	public final void testFileOverwrite() {
		Scanner input4 = new Scanner(System.in);
		assertEquals("study.ics", CalendarDriver.getFileName(input4));
	}
	
	@Test
	public final void testEmptyNameFile() {
		Scanner input5 = new Scanner(System.in);
		assertEquals(".ics", CalendarDriver.getFileName(input5));
	}
	
	@Test
	public final void testGetClassificationField() {
		Scanner input6 = new Scanner(System.in);
		Calendar cal1 = new Calendar();
		CalendarDriver.getClassificationField(input6, cal1);
		assertEquals("PUBLIC", cal1.getClassification());
		
		CalendarDriver.getClassificationField(input6, cal1);
		assertEquals("PRIVATE", cal1.getClassification());
		
		CalendarDriver.getClassificationField(input6, cal1);
		assertEquals("CONFIDENTIAL", cal1.getClassification());
	}
	
	@Test
	public final void testNumberInputGetClassification(){
		Scanner input7 = new Scanner(System.in);
		Calendar cal2 = new Calendar(); 
		CalendarDriver.getClassificationField(input7, cal2);
		assertNotEquals("4325", cal2.getClassification());
	}
	
	@Test
	public final void testCharInputGetClassification(){
		Scanner input7 = new Scanner(System.in);
		Calendar cal2 = new Calendar(); 
		CalendarDriver.getClassificationField(input7, cal2);
		assertNotEquals("$#@", cal2.getClassification());
	}
	
	@Test
	public final void testEmptyInputGetClassification(){
		Scanner input8 = new Scanner(System.in);
		Calendar cal3 = new Calendar();
		CalendarDriver.getClassificationField(input8, cal3);
		assertNotEquals("", cal3.getClassification());
	}

	@Test
	public final void testGetLocationField() {
		Scanner input4 = new Scanner(System.in);
		Calendar cal2 = new Calendar();
		CalendarDriver.getLocationField(input4, cal2);
		assertEquals("Hamilton library", cal2.getLocation());
		
		CalendarDriver.getLocationField(input4, cal2);
		assertNotEquals("Sinclair", cal2.getLocation());
	}

	@Test
	public final void testGetPriorityField() {
		Scanner input5 = new Scanner(System.in);
		Calendar cal6 = new Calendar();
		CalendarDriver.getPriorityField(input5, cal6);
		assertEquals(7,cal6.getPriority());
	}
	
	@Test
	public final void testCharInputGetPriority() {
		Scanner input6 = new Scanner(System.in);
		Calendar cal7 = new Calendar();
		CalendarDriver.getPriorityField(input6, cal7);
		assertNotEquals("dog", cal7.getPriority());
	}
	
	@Test
	public final void testCharAndNumberInputGetPriority() {
		Scanner input7 = new Scanner(System.in);
		Calendar cal8 = new Calendar();
		CalendarDriver.getPriorityField(input7, cal8);
		assertNotEquals("3RT", cal8.getPriority());
	}
	
	@Test
	public final void testGetSummaryField() {
		Scanner input8 = new Scanner(System.in);
		Calendar cal9 = new Calendar();
		CalendarDriver.getSummaryField(input8, cal9);
		assertEquals("Studying Finals", cal9.getSummary());
	}

	@Test
	public final void testGetStartDateTime() {
		Scanner input9 = new Scanner(System.in);
		Calendar cal10 = new Calendar();
		CalendarDriver.getStartDateTime(input9, cal10);
		assertEquals("20150515T061200",cal10.getDTSTART());
	}

	@Test
	public final void testErrorInputGetTime(){
		Scanner input10 = new Scanner(System.in);
		Calendar cal11 = new Calendar();
		CalendarDriver.getStartDateTime(input10, cal11);
		assertNotEquals("May, 4 2014 05:00",cal11.getDTSTART());
	}
	
	@Test
	public final void testIsValidDate() {
		assertTrue(CalendarDriver.isValidDate("05/12/2015"));
	}

	@Test
	public final void testInvalidWordDate() {
		assertFalse(CalendarDriver.isValidDate("May 15 2014"));
	}
	
	@Test
	public final void testInvalidMonthStartDate() {
		assertFalse(CalendarDriver.isValidDate("23/05/3214"));
	}
	
	@Test
	public final void testInvalidDayStartDate() {
		assertFalse(CalendarDriver.isValidDate("04/34/3214"));
	}
	
	@Test
	public final void testValidYearStartDate() {
		assertFalse(CalendarDriver.isValidDate("04/31/0000"));
	}
	
	@Test
	public final void testDateLengthStartDate() {
		assertFalse(CalendarDriver.isValidDate("04/31/0000342"));
	}
	
	@Test
	public final void testDateFormatStartDate() {
		assertFalse(CalendarDriver.isValidDate("04311900"));
	}
	
	@Test
	public final void testIsValidTimeStartDate() {
		assertTrue(CalendarDriver.isValidTime("06:50"));
	}

	@Test
	public final void testInvalidHourStartDate() {
		assertFalse(CalendarDriver.isValidTime("25:50"));
	}
	
	@Test
	public final void testInvalidMinuteStartDate() {
		assertFalse(CalendarDriver.isValidTime("12:67"));
	}
	
	@Test
	public final void testTimeLengthStartDate() {
		assertFalse(CalendarDriver.isValidTime("12:6742"));
	}
	
	@Test
	public final void testGetEndDateTime() {
		Scanner input11 = new Scanner(System.in);
		Calendar cal12 = new Calendar();
		CalendarDriver.getStartDateTime(input11, cal12);
		CalendarDriver.getEndDateTime(input11, cal12);
		assertEquals("20150717T052000",cal12.getDTEND());
	}

	@Test
	public final void testDatePrecedeErrorByMonth() {
		Scanner input13 = new Scanner(System.in);
		Calendar cal14 = new Calendar();
		CalendarDriver.getStartDateTime(input13, cal14);
		//start time = 05/15/2015, 06:30
		CalendarDriver.getEndDateTime(input13, cal14);
		assertNotEquals("20150416T052000",cal14.getDTEND());
	}
	
	@Test
	public final void testDatePrecedeErrorByDay() {
		Scanner input14 = new Scanner(System.in);
		Calendar cal15 = new Calendar();
		CalendarDriver.getStartDateTime(input14, cal15);
		//start time = 05/15/2015, 06:30
		CalendarDriver.getEndDateTime(input14, cal15);
		assertNotEquals("20150505T052000",cal15.getDTEND());
	}
	
	@Test
	public final void testDatePrecedeErrorByYear() {
		Scanner input15 = new Scanner(System.in);
		Calendar cal16 = new Calendar();
		CalendarDriver.getStartDateTime(input15, cal16);
		//start time = 05/15/2015, 06:30
		CalendarDriver.getEndDateTime(input15, cal16);
		assertNotEquals("19900505T052000",cal16.getDTEND());
	}
	
	@Test
	public final void testInvalidMonthEndDate() {
		Scanner input16 = new Scanner(System.in);
		Calendar cal17 = new Calendar();
		CalendarDriver.getStartDateTime(input16, cal17);
		//start time = 05/15/2015, 06:30
		CalendarDriver.getEndDateTime(input16, cal17);
		assertNotEquals("19900505T052000",cal17.getDTEND());
	}
	
	@Test
	public final void testInvalidDayEndDate() {
		Scanner input16 = new Scanner(System.in);
		Calendar cal17 = new Calendar();
		CalendarDriver.getStartDateTime(input16, cal17);
		//start time = 05/15/2015, 06:30
		CalendarDriver.getEndDateTime(input16, cal17);
		assertNotEquals("19900545T052000",cal17.getDTEND());
	}
	
	@Test
	public final void testInvalidHourEndDate() {
		Scanner input16 = new Scanner(System.in);
		Calendar cal17 = new Calendar();
		CalendarDriver.getStartDateTime(input16, cal17);
		//start time = 05/15/2015, 06:30
		CalendarDriver.getEndDateTime(input16, cal17);
		assertNotEquals("19900505T252000",cal17.getDTEND());
	}
	
	@Test
	public final void testInvalidMinuteEndDate() {
		Scanner input16 = new Scanner(System.in);
		Calendar cal17 = new Calendar();
		CalendarDriver.getStartDateTime(input16, cal17);
		//start time = 05/15/2015, 06:30
		CalendarDriver.getEndDateTime(input16, cal17);
		assertNotEquals("19900505T057200",cal17.getDTEND());
	}
	
	@Test
	public final void testValidEndDate() {
		assertTrue(CalendarDriver.validEndDate("20150515T063000", "06/31/2016"));
	}

	@Test
	public final void testPrecedeMonth() {
		assertFalse(CalendarDriver.validEndDate("20150515T063000", "01/15/2015"));
	}
	
	@Test
	public final void testPrecedeDay() {
		assertFalse(CalendarDriver.validEndDate("20150515T063000", "05/01/2015"));
	}
	
	@Test
	public final void testPrecedeYear() {
		assertFalse(CalendarDriver.validEndDate("20150515T063000", "05/15/2012"));
	}
	
	@Test
	public final void testInvalidMonth() {
		assertFalse(CalendarDriver.validEndDate("20150515T063000", "55/15/2012"));
	}
	
	@Test
	public final void testInvalidDay() {
		assertFalse(CalendarDriver.validEndDate("20150515T063000", "05/32/2012"));
	}
		
	@Test
	public final void testValidEndTime() {
		assertTrue(CalendarDriver.validEndTime("20150515T063000", "20150615","06:50"));
	}

	//error
	@Test
	public final void testPrecedeDate() {
		assertFalse(CalendarDriver.validEndTime("20150515T063000", "20150415","06:50"));
	}
	
	@Test
	public final void testPrecedeHour() {
		assertFalse(CalendarDriver.validEndTime("20150515T063000", "20150515","05:50"));
	}
	
	@Test 
	public final void testPrecedeMinute() {
		assertFalse(CalendarDriver.validEndTime("20150515T063000", "20150515","06:15"));
	}
	
	//error
	@Test
	public final void testInvalidHour() {
		assertFalse(CalendarDriver.validEndTime("20150515T063000", "20150515","25:15"));
	}
	
	//error +14, +13
	@Test
	public final void testGetTimeZoneField() {
		Scanner input17 = new Scanner(System.in);
		Calendar cal18 = new Calendar();
		CalendarDriver.getTimeZoneField(input17, cal18);
		assertEquals("Etc/GMT+10", cal18.getTimeZone());
	}

	@Test
	public final void testCharInputGetTimeZoneField() {
		Scanner input18 = new Scanner(System.in);
		Calendar cal19 = new Calendar();
		CalendarDriver.getTimeZoneField(input18, cal19);
		assertNotEquals("Etc/GMT dog", cal19.getTimeZone());
	}
	
}