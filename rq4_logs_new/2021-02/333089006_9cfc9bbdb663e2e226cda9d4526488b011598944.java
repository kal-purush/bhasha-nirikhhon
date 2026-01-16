package game;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TestAbilityDamage {

	@Test
	void testBossHealth() {
		AbilityDamage a = new AbilityDamage();
		assertEquals(a.getBossHealth(), 300);
	}
	
	@Test
	void testPlayerHealth() {
		AbilityDamage a = new AbilityDamage();
		ChooseYourCharacter b = new ChooseYourCharacter();
		TheStory c = new TheStory();
		c.Story();
		int i = 0;
		assertTrue(a.getBossHealth() <= 0);
	}

}