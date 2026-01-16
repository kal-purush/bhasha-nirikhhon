package com.gameEngine.commands;

import com.character.Player;
import com.gameEngine.GameEngine;
import com.item.Item;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class GOCommandTest
{
    private StringBuilder gameBuilder ;
    private String[] command ;
    private Map<String, Map<String, List<String>>> instructs;
    private CommandInterface goCommand;
    private Item item;
    private Player player;
    @Before
    public void setUp() throws IOException {
        gameBuilder = new StringBuilder();
        command = new String[2];
        command[0] = "get";
        command[1] = "test item";
        item = new Item("test item", "Landing Dock", "a test item");
        instructs= GameEngine.GAME_ENGINE.getInstructs();
        goCommand = new GOCommand();
        player = Player.PLAYER;

    }
//
//    @Test
//    public void processCommand_shouldChangeLocations_whenValidLocationGiven() {
//        String oldLocation = player.getCurrentLocation().getName();
//        goCommand.processCommand(gameBuilder,command,instructs);
//        assertTrue(!oldLocation.equalsIgnoreCase(player.getCurrentLocation().getName()));
//    }
}