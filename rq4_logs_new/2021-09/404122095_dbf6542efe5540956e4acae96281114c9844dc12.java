package com.gameEngine.commands;

import com.gameEngine.GameEngine;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CATALOGCommandTest {
    private StringBuilder gameBuilder ;
    private String[] command ;
    private Map<String, Map<String, List<String>>> instructs;
    private CommandInterface catalogCommand;

    @Before
    public void setUp() {
        gameBuilder = new StringBuilder();
        command = new String[2];
        command[0] = "catalog";
        instructs= GameEngine.GAME_ENGINE.getInstructs();
        catalogCommand = new CATALOGCommand();
    }

    @Test
    public void processCommand_shouldUpdateStringBuilder_whenPassed() {
        catalogCommand.processCommand(gameBuilder,command,instructs);
        assertTrue(gameBuilder.toString().contains("the items locations"));
    }



}