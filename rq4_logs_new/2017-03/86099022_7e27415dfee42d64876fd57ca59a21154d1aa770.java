package bookkeeper;

import static org.junit.Assert.*;


import org.junit.Test;

public class AuthorTest {

	@Test
	public void equalTest() 
	{
		Author JoeSmith = new Author("Smith","Joe");
		assertFalse(JoeSmith.equals(null));
		Author JoeSmith2 = new Author("Smith","Joe");
		Author FrankSmith = new Author("Smith","Frank");
		Author JoeRyan = new Author("Ryan", "Joe");
		assertTrue(JoeSmith.equals(JoeSmith2));
		assertFalse(JoeSmith.equals(FrankSmith));
		assertFalse(JoeSmith.equals(JoeRyan));
		Author JoeEarlSmith = new Author("Smith", "Joe", "Earl");
		Author JoeEarlSmith2 = new Author("Smith", "Joe", "Earl");
		assertTrue(JoeEarlSmith.equals(JoeEarlSmith2));
		assertFalse(JoeEarlSmith.equals(JoeSmith));
		assertFalse(JoeSmith.equals(JoeEarlSmith));
		
	}
	@Test
	public void compareToTest() 
	{
		Author JoeSmith = new Author("Smith","Joe");
		Author ZeldaAddams = new Author("Addams","Zelda");
		assertTrue(ZeldaAddams.compareTo(JoeSmith) < 0);
		Author AdamSmith = new Author("Smith","Adam");
		assertTrue(AdamSmith.compareTo(JoeSmith) < 0);
		Author JoeSmith2 = new Author("Smith","Joe");
		assertTrue(JoeSmith2.compareTo(JoeSmith) == 0);
		Author JoeEarlSmith = new Author("Smith", "Joe", "Earl");
		assertTrue(JoeSmith.compareTo(JoeEarlSmith) < 0);
		Author JoeEarlSmith2 = new Author("Smith", "Joe", "Earl");
		assertTrue(JoeEarlSmith.compareTo(JoeEarlSmith2) == 0);
	}
	
}