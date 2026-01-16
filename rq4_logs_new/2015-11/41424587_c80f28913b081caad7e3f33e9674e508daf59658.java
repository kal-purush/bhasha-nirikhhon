//@@author A0124127R
package test;

import static org.junit.Assert.*;

import org.junit.Test;
import application.Constants;
import parser.Parser;

public class CommandParserTest {
	
	Parser parser = new Parser();
	
	public static final String VALID_COMMAND = "Valid Command";
	public static final String INVALID_COMMAND = "Invalid command";
	
	/*
	 * Tests command inputs and their variations(if any),
	 * and checks if the command given is accurate
	 */
	
	@Test
	public void testGetCommand_Add() { 			// "add" and command variations
		
		String input1 = new String("add task1");
		String input2 = new String("create task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_ADD);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_ADD);
		
	}
	@Test
	public void testGetCommand_Delete() {		// "delete" and command variations

		String input1 = new String("delete task1");
		String input2 = new String("del task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_DELETE);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_DELETE);
		
	}
	@Test
	public void testGetCommand_Edit() {			// "edit" and command variations

		String input1 = new String("edit task1");
		String input2 = new String("e task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_EDIT);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_EDIT);
		
	}
	@Test
	public void testGetCommand_Undo() {			// "undo"

		String input1 = new String("undo");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_UNDO);
		
	}
	@Test
	public void testGetCommand_Redo() {			// "redo"

		String input1 = new String("redo");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_REDO);
	
	}
	@Test
	public void testGetCommand_Mark() {			// "mark" and command variations

		String input1 = new String("mark task1");
		String input2 = new String("m task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_MARK);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_MARK);
	
	}
	@Test
	public void testGetCommand_Unmark() {		// "unmark" and command variations

		String input1 = new String("unmark task1");
		String input2 = new String("um task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_UNMARK);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_UNMARK);
	
	}
	@Test
	public void testGetCommand_EnquirePath() {	// "enquirepath" and command variations

		String input1 = new String("enquirepath");
		String input2 = new String("en");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_ENQUIREPATH);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_ENQUIREPATH);
	
	}
	@Test
	public void testGetCommand_Help() {			// "help"

		String input1 = new String("help");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_HELP);
	
	}
	@Test
	public void testGetCommand_Show() {			// "show" and command variations

		String input1 = new String("show task1");
		String input2 = new String("display task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_SHOW);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_SHOW);
	
	}
	@Test
	public void testGetCommand_Search() {			// "show" and command variations

		String input1 = new String("search task1");
		String input2 = new String("find task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_SEARCH);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_SEARCH);
	
	}
	
	/*
	 * Boundary Test Cases for getCommand()
	 */
	
	@Test
	public void testGetCommand_CommandBoundary1() {			// input commands is within the list

		String input1 = new String("add task1");
		String input2 = new String("find task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_ADD);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_SEARCH);
	
	}

	/*
	 * Fails because command is only registered if it's exact
	 */
	
	@Test
	public void testGetCommand_CommandBoundary2() {			// input command is incomplete / has spaces in between

		String input1 = new String("ad d task1");
		String input2 = new String("sear task2");
		
		assertFalse(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_ADD);
		assertFalse(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_SEARCH);
		assertTrue(VALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_INVALID);
		assertTrue(VALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_INVALID);
	
	}
	
	/*
	 * Fails due to incorrect command words
	 */
	
	@Test
	public void testGetCommand_CommandBoundary3() {			// input command contains incorrect characters

		String input1 = new String("cr3ate task1");
		String input2 = new String("searchhh task2");
		
		assertFalse(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_ADD);
		assertFalse(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_SEARCH);
		assertTrue(VALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_INVALID);
		assertTrue(VALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_INVALID);
	
	}
	
	@Test
	public void testGetCommand_CommandBoundary4() {			// input commands has white spaces before text

		String input1 = new String(" add task1");
		String input2 = new String("    find task2");
		
		assertTrue(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_ADD);
		assertTrue(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_SEARCH);
	
	}
	
	/*
	 * Fails as commands are only registered as the first word
	 */
	
	@Test
	public void testGetCommand_CommandBoundary5() {			// input commands has text before commands

		String input1 = new String("text mark task1");
		String input2 = new String("word show task2");
		
		assertFalse(INVALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_MARK);
		assertFalse(INVALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_SHOW);
		assertTrue(VALID_COMMAND, parser.getCommand(input1) == Constants.COMMAND_INVALID);
		assertTrue(VALID_COMMAND, parser.getCommand(input2) == Constants.COMMAND_INVALID);
	
	}
}