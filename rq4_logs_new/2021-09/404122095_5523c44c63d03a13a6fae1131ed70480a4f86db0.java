package com.engine.commands;

import com.character.Player;
import com.gameEngine.GameEngine;
import com.gameEngine.commands.CommandInterface;
import com.gameEngine.commands.GETCommand;
import com.gameEngine.commands.GOCommand;
import com.item.Item;
import com.location.Locations;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class GOCommandTest {
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
        command[0] = "go";
        command[1] = "north";
        item = new Item("test item", "Landing Dock", "a test item");
        instructs= GameEngine.GAME_ENGINE.getInstructs();
        goCommand = new GOCommand();
        player = Player.PLAYER;
        player.setCurrentLocation(Locations.LandingDock);
        Locations.LandingDock.setName("Landing Dock");

    }

    @Test
    public void processCommand_shouldChangeLocations_whenValidLocationGiven() {

        String oldLocationName = player.getCurrentLocation().getName();
        goCommand.processCommand(gameBuilder,command,instructs);
        assertEquals(player.getCurrentLocation().getName(), oldLocationName);
    }

    @Test
    public void processCommand_shouldNotChangeLocations_whenInvalidLocationGiven() {

    }
}