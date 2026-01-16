package com.gameEngine;

import com.character.Player;
import com.engine.Instruction;
import com.engine.Parser;
import com.gameEngine.commands.*;
import com.item.Item;
import com.location.Locations;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public enum GameEngine {

    GAME_ENGINE; // how to refer to instance of game engine
    private Locations currentLocation;
    private final Player player = Player.PLAYER;
    private StringBuilder gameBuilder = new StringBuilder();

    public StringBuilder getGameBuilder() {
        return gameBuilder;
    }

    public void setGameBuilder(StringBuilder gameBuilder) {
        this.gameBuilder = gameBuilder;
    }

    //private Instruction instructs;
    private Map<String, Map<String, List<String>>> instructs =new Instruction().getInstruction();

    //public StringBuilder status = showStatus(currentLocation);
    public List<Item> inventory;

    //NPC zombies; ?? later for tracking how many are alive and where?
    HashMap<String, Item> catalog = Item.readAll();
    public StringBuilder runGameLoop(String input) {

        StringBuilder gameBuilder = new StringBuilder();
        inventory = player.getInventory();

        String[] command;
        command = Parser.parseInput(input.toLowerCase());


        ArrayList<CommandInterface> commandList = getCommandInterfaces();

        for(CommandInterface commandFromList : commandList){
            if(commandFromList.getClass().getSimpleName().contains(command[0].toUpperCase())){
                commandFromList.processCommand();
            }
        }
        //update win/lose status
        checkPlayerHealth();
        checkPuzzleComplete();
        return gameBuilder.append(showStatus(currentLocation.getName()));
    }



    //    //This method finds out if there are zombies in that current location using the JSON file
    private Boolean checkForZombies() {
        try {
            JSONObject current = getJsonObject();
            String zombie = (String) current.get("enemy");
            if ("Zombie".equals(zombie) ) {
                return true;
            }
        } catch (NullPointerException e) {
            System.out.println("No zombies in " + currentLocation);
        }
        return false;
    }

    public StringBuilder showStatus(String location) {
        StringBuilder builder = new StringBuilder();
        builder.append(instructs.get("status").get("instructions").get(1))
                .append(location).append("\n\n");
        return builder;
    }

    private void checkPlayerHealth() {
        //if Player.getHealth() < 1 { loseStatus = true; }
    }

    private void checkPuzzleComplete() {
        //if wizzlewhat and space wrench are in player inventory, winStatus = true
        //later, account for fueling time as well
    }



    private JSONObject getJsonObject() {
        JSONObject locations = new JSONObject();
        try {
            JSONParser parser = new JSONParser();
            locations = (JSONObject) parser.parse(new FileReader("cfg/Locations.json"));

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
        return (JSONObject) locations.get(currentLocation);
    }
    public void setCurrentLocation(Locations currentLocation) {
        this.currentLocation = currentLocation;
    }
    private ArrayList<CommandInterface> getCommandInterfaces() {
        CommandInterface qCommand = new QCommand();
        CommandInterface talkCommand = new TALKCommand();
        CommandInterface lookCommand = new LOOKCommand();
        CommandInterface fightCommand =  new FIGHTCommand();
        CommandInterface useCommand = new USECommand();
        CommandInterface getCommand = new GETCommand();
        CommandInterface dropCommand = new DROPCommand();
        CommandInterface goCommand = new GOCommand();
        CommandInterface catalogCommand = new CATALOGCommand();
        CommandInterface inventoryCommand = new INVENTORYCommand();
        CommandInterface helpCommand = new HELPCommand();
        ArrayList<CommandInterface> commandList = new ArrayList();
        commandList.add(qCommand);
        commandList.add(talkCommand);
        commandList.add(lookCommand);
        commandList.add(fightCommand);
        commandList.add(useCommand);
        commandList.add(getCommand);
        commandList.add(dropCommand);
        commandList.add(goCommand);
        commandList.add(catalogCommand);
        commandList.add(inventoryCommand);
        commandList.add(helpCommand);
        return commandList;


    }

    public Player getPlayer() {
        return player;
    }

    public Map<String, Map<String, List<String>>> getInstructs() {
        return instructs;
    }

    public List<Item> getInventory() {
        return inventory;
    }

    public HashMap<String, Item> getCatalog() {
        return catalog;
    }



}