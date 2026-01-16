package assignment3;

import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;

public class TestGeneral {

	@Test
	public void testIfNotEmptyBFS() {

		assertNotEquals(Collections.EMPTY_LIST, Main.getWordLadderBFS("SMART", "HONEY"));
	}

	@Test
	public void testIfDifSizeBFS() {

		assertEquals(Collections.EMPTY_LIST, Main.getWordLadderBFS("SMART", ""));
	}

	@Test
	public void testIfSameWordBFS() {

		assertEquals(1, Main.getWordLadderBFS("MONEY", "MONEY").size());
	}

	@Test
	public void testIfEmptyBFS() {

		assertEquals(Collections.EMPTY_LIST, Main.getWordLadderBFS("", "FUNNY"));
	}

	@Test
	public void testIfFirstLetterDifBFS() {

		assertEquals(2, Main.getWordLadderBFS("MONEY", "HONEY").size());
	}

	@Test
	public void testIfNotEmptyDFS() {

		assertNotEquals(Collections.EMPTY_LIST, Main.getWordLadderBFS("SMART", "HONEY"));
	}

	@Test
	public void testIfDifSizeDFS() {

		assertEquals(Collections.EMPTY_LIST, Main.getWordLadderDFS("SMART", "CENA"));
	}

	@Test
	public void testIfSameWordDFS() {

		assertEquals(1, Main.getWordLadderDFS("MONEY", "MONEY").size());
	}

	@Test
	public void testIfEmptyDFS() {

		assertEquals(Collections.EMPTY_LIST, Main.getWordLadderDFS("", "FUNNY"));
	}

	@Test
	public void testIfFirstLetterDifDFS() {

		assertEquals(2, Main.getWordLadderDFS("MONEY", "HONEY").size());
	}

}