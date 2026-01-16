package Testing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import card.Card;

public class TestCard {
	
	//Check flip
	@Test
	public void testPrint0() {
		Card result = new Card(13,1);
		result.setShow(false);
		assertEquals("??", result.print());
	}
	
	//Check suit
	@Test
	public void testPrint1() {
		Card result = new Card(13,1);
		result.setShow(true);
		assertEquals("♠K", result.print());
	}
	
	@Test
	public void testPrint2() {
		Card result = new Card(13,2);
		result.setShow(true);
		assertEquals("♥K", result.print());
	}
	
	@Test
	public void testPrint3() {
		Card result = new Card(13,3);
		result.setShow(true);
		assertEquals("♣K", result.print());
	}
	
	@Test
	public void testPrint4() {
		Card result = new Card(13,4);
		result.setShow(true);
		assertEquals("♦K", result.print());
	}
	
	@Test
	public void testPrint5() {
		Card result = new Card(13,5);
		result.setShow(true);
		assertEquals("ErrorK", result.print());
	}
	
	//Check rank
	@Test
	public void testPrint6() {
		Card result = new Card(12,2);
		result.setShow(true);
		assertEquals("♥Q", result.print());
	}
	
	@Test
	public void testPrint7() {
		Card result = new Card(11,2);
		result.setShow(true);
		assertEquals("♥J", result.print());
	}
	
	@Test
	public void testPrint8() {
		Card result = new Card(1,2);
		result.setShow(true);
		assertEquals("♥A", result.print());
	}
	
	@Test
	public void testPrint9() {
		Card result = new Card(2,2);
		result.setShow(true);
		assertEquals("♥2", result.print());
	}
	
}