package com.gameEngine.commands;

import com.character.Player;
import com.character.Zombie;
import com.gameEngine.GameEngine;
import com.location.Locations;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class FIGHTCommandTest {

    private StringBuilder gameBuilder ;
    private String[] command ;
    private Map<String, Map<String, List<String>>> instructs;
    private CommandInterface fightCommand;

    private Zombie zombie;


    @Before
    public void setUp() {
        gameBuilder = new StringBuilder();
        command = new String[2];
        command[0] = "fight";
        instructs= GameEngine.GAME_ENGINE.getInstructs();
        fightCommand = new FIGHTCommand();

        zombie = Zombie.getInstance();


    }

    @Test
    public void processFight_shouldChangeHealthOfZombieAndPlayer_whenCalledAndZombiePresent() {
        int oldPlayerHealth = Player.PLAYER.getHealth();
        int oldZombieHealth = zombie.getHealth();
        Locations.LandingDock.setName("Landing Dock");
        Locations.LandingDock.setZombie(zombie);
        zombie.setLocation("Landing Dock");
        fightCommand.processCommand(gameBuilder,command,instructs);
        assertTrue(Player.PLAYER.getHealth() < oldPlayerHealth);
        assertTrue(zombie.getHealth() < oldZombieHealth);

    }
    @Test
    public void processFight_shouldChangeHealthOfZombieAndPlayer_whenCalledAndZombiePresentPlusCommandOneIsZombie() {
        command[1] = "zombie";
        int oldPlayerHealth = Player.PLAYER.getHealth();
        int oldZombieHealth = zombie.getHealth();
        Locations.LandingDock.setName("Landing Dock");
        Locations.LandingDock.setZombie(zombie);
        zombie.setLocation("Landing Dock");
        fightCommand.processCommand(gameBuilder,command,instructs);
        assertTrue(Player.PLAYER.getHealth() < oldPlayerHealth);
        assertTrue(zombie.getHealth() < oldZombieHealth);

    }
    @Test
    public void processFight_shouldNotChangeHealthOfZombieAndPlayer_whenCalledAndZombieNotPresent() {
        int oldPlayerHealth = Player.PLAYER.getHealth();
        int oldZombieHealth = zombie.getHealth();
        Locations.LandingDock.setName("Landing Dock");

        fightCommand.processCommand(gameBuilder,command,instructs);
        assertTrue(Player.PLAYER.getHealth() == oldPlayerHealth);
        assertTrue(zombie.getHealth() == oldZombieHealth);

    }

    @Test
    public void processFight_shouldGiveFeedBackThroughGameBuilder_whenItDoesNotPerformAttack() {
        Locations.LandingDock.setName("Central Hub");

        fightCommand.processCommand(gameBuilder,command,instructs);
        assertTrue(gameBuilder.toString().contains(instructs.get("fight").get("instructions").get(9)));
    }

    @Test
    public void processFight_shouldNullifyZombieAtLocation_whenZombieKilled() {
        Locations.LandingDock.setName("Landing Dock");
        fightCommand.processCommand(gameBuilder,command,instructs);
        assertTrue(Player.PLAYER.getCurrentLocation().getZombie() == null);
    }

}