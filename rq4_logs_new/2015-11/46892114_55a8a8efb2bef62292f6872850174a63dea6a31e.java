package net.franckbenault.testcollection.tabs;

import static org.junit.Assert.*;

import org.junit.Test;

public class TabsTestCase {

	@Test
	public void testLength() {
		String[] tab = new String[3];
	    assertEquals(tab.length,3);
	}
	
	@Test(expected=ArrayIndexOutOfBoundsException.class)
	public void test() {
		String[] tab = new String[3];
	    tab[0] = "One";
	    tab[1] = "Two";
	    tab[2] = "Three";
	    //error the size of the tab is 3
	    tab[3] = "Four";
	}

}