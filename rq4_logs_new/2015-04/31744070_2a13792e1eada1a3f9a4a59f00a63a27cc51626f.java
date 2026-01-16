package calendar;

import static org.junit.Assert.*;

import java.util.Scanner;

import org.junit.Test;

import java.io.File;

public class CommonFreeTimeDriverTest {

	@Test
	public final void testFileDoesNotExists() {
		File testFile = new File("test.ics");
		String [] testArray = new String[2];
		testArray[0] = "test1";
		testArray[1] = "test2";
		assertFalse(CommonFreeTimeDriver.fileExists(testFile, ".ics", testArray, 0));
	}

	@Test
	public final void testFileExists() {
		File testFile = new File("class2.ics");
		String [] testArray = new String[2];
		testArray[0] = "test1.ics";
		testArray[1] = "test2.ics";
		assertTrue(CommonFreeTimeDriver.fileExists(testFile, ".ics", testArray, 0));
	}
	
	@Test
	public final void testHasExtension() {
		assertTrue(CommonFreeTimeDriver.hasExtension("ics"));	
	}
	
	@Test
	public final void testDoesNotHaveExtension() {
		assertFalse(CommonFreeTimeDriver.hasExtension("test"));	
	}
	
	@Test
	public final void testValidFiles(){
	    FreeTimeFinder freeTime = null;
	    String[] fileNames = null, fileNames1 = null, fileNames2 = null;
	    Scanner keybd = new Scanner(System.in);
	    assertTrue(CommonFreeTimeDriver.initiateProgram(freeTime, fileNames, fileNames1, fileNames2, keybd));
	}
	
	@Test
	public final void testInvalidFiles(){
	    FreeTimeFinder freeTime = null;
	    String[] fileNames = null, fileNames1 = null, fileNames2 = null;
	    Scanner keybd = new Scanner(System.in);
	    assertFalse(CommonFreeTimeDriver.initiateProgram(freeTime, fileNames, fileNames1, fileNames2, keybd));
	}
}