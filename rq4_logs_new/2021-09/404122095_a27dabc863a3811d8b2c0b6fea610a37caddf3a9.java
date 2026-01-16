package com.gameEngine.commands;

import com.gameEngine.GameEngine;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class HELPCommandTest {
    private StringBuilder gameBuilder ;
    private String[] command ;
    private Map<String, Map<String, List<String>>> instructs;
    private CommandInterface helpCommand;

    @Before
    public void setUp() {
        gameBuilder = new StringBuilder();
        command = new String[2];
        command[0] = "help";
        instructs= GameEngine.GAME_ENGINE.getInstructs();
        helpCommand = new HELPCommand();
    }

    @Test
    public void processCommand_shouldUpdateStringBuilder_whenProcessCommandExecuted() {
        helpCommand.processCommand(gameBuilder, command, instructs);
        assertTrue(gameBuilder.toString().contains("Movement: go (north, south, east, west)"));
    }
}