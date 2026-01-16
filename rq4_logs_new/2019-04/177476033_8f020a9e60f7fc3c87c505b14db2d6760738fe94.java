package info;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class HistoryTest {
	
	@Test
	public void test() {
		History h1 = new History("chicken");
		assertEquals("chicken", h1.query);
		History h2 = new History("chicken", 1, 2000);
		assertEquals("chicken", h2.query);
		assertEquals(1, h2.number);
		assertEquals(2000, h2.radius);
		History h3 = new History(1, "chicken", 1, 2000);
		assertEquals(1, h3.key);
		assertEquals("chicken", h3.query);
		assertEquals(1, h3.number);
		assertEquals(2000, h3.radius);
	}
}