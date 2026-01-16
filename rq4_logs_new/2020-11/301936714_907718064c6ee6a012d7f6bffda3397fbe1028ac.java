package skunk.domain;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestPlayer
{
	
	
	// Test to verify Player's score 

	@Test
	public void testPlayer1()
	{
		Player player = new Player();	
		assertEquals("testPlayer1", 0, player.getPlayerScore());
	}
	
	// Test to verify Player's name
	
	@Test	
	public void testPlayer2() {
		
		Player player1 = new Player("test");
		assertEquals("test",player1.getPlayerName());
		
	}
	
	// Test to verify Player's name is a String
	
	@Test
	public void testPlayerString() 
	{	
		Player player1 = new Player("test");
		assertTrue(player1 instanceof Player);
	}
	
	// Test to test set chips for Player
	
	@Test
	public void testsetPlayerChipCount() 
	{
		Player player1 = new Player("test");
		player1.setPlayerChipCount(5);
		assertTrue(player1.getPlayerChipCount() == 5);
	}

}