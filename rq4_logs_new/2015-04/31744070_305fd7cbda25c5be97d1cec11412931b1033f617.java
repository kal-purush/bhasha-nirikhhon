package calendar;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;


public class FreeTimeFinderTest {
	
	String f1 = "study0.ics";
	String f2 = "study1.ics";
	String f3 = "study2.ics";
	
	String[] files = { f1, f2, f3 };
	
	
	@Test
	public final void testFreeTimeFinder() {
		FreeTimeFinder ftf1 = new FreeTimeFinder(files);
		assertNotNull(ftf1);
	}
	
	@Test
	public void testGetSize() {
		FreeTimeFinder ftf2 = new FreeTimeFinder(files);
		assertEquals(0, ftf2.getSize());
	}
	
	
	@Test
	public void testGetFreeEvent() throws IOException {
		FreeTimeFinder ftf3 = new FreeTimeFinder(files);
		ftf3.calculateBusyTimes();
		ftf3.calculateFreeTimes();
		assertNotNull(ftf3.getFreeEvent(0));
	}
	
	@Test (expected = IndexOutOfBoundsException.class)
	public void testGetFreeEventError() throws IOException {
		FreeTimeFinder ftf3a = new FreeTimeFinder(files);
		ftf3a.getFreeEvent(0);
	}

	@Test
	public void testCalculateBusyTimes() throws IOException {
		FreeTimeFinder ftf4 = new FreeTimeFinder(files);
		ftf4.calculateBusyTimes();
	}
	
	@Test (expected = IOException.class)
	public void testCalculateBusyTimesError() throws IOException {
		String invalid = "invalid";
		files[2] = invalid;
		FreeTimeFinder ftf4 = new FreeTimeFinder(files);
		ftf4.calculateBusyTimes();
	}

	@Test
	public void testCalculateFreeTimes() throws IOException {
		FreeTimeFinder ftf5 = new FreeTimeFinder(files);
		ftf5.calculateBusyTimes();
		ftf5.calculateFreeTimes();
		assertNotNull(ftf5.getFreeEvent(0));
	}

	@Test (expected = NullPointerException.class)
	public void testCreateEvent() {
		FreeTimeFinder ftf6 = new FreeTimeFinder(files);
		ftf6.createEvent("20150508T000000", "20150508T180000");
	}
	
	@Test (expected = NullPointerException.class)
	public void testCreateEventError() {
		FreeTimeFinder ftf6 = new FreeTimeFinder(files);
		ftf6.createEvent("5/12/15", "5/12/15");
	}

}