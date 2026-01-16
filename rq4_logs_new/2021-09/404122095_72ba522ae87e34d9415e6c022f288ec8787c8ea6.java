package com.gameEngine.commands;

import com.character.Player;
import com.gameEngine.GameEngine;
import com.item.Item;
import com.item.Weapon;
import com.location.Locations;
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
    private Item item;
    private Player player;
    @Before
    public void setUp() throws Exception {
        Item.getItems("cfg/Items.json");
        Weapon.getWeapons("cfg/Weapons.json");
        Locations.initWithJsonFile("cfg/sampleLocations.json");
        gameBuilder = new StringBuilder();
        command = new String[2];
        command[0] = "help";
        command[1] = "";
        instructs= GameEngine.GAME_ENGINE.getInstructs();
        helpCommand = new HELPCommand();
        player = Player.PLAYER;
        player.setCurrentLocation(Locations.LandingDock);
    }

    @Test
    public void processCommand_shouldGiveTheHelp_whenHelpIsCalled() {
        helpCommand.processCommand(gameBuilder,command,instructs);
        assertTrue(gameBuilder.toString().contains(instructs.get("help").get("instructions").get(0)));
    }
}