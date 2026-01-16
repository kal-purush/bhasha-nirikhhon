package com.engine.commands;

import com.character.Player;
import com.character.Zombie;
import com.item.Item;
import org.json.simple.parser.JSONParser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CATALOGCommand implements CommandInterface {


    @Override
    public void processCommand(StringBuilder gameBuilder, String[] command, String currentLocation, List<Item> inventory, Zombie zombieNPC, Map<String, Map<String, List<String>>> instructs, Player player, HashMap<String, Item> catalog, JSONParser parser) {
        gameBuilder.append(reviewCatalog(catalog));
    }

    private static String reviewCatalog(HashMap<String, Item> catalog) {
        String returnVal = " Items : [";
        for (Map.Entry<String, Item> item : catalog.entrySet()) {
            returnVal += item.getValue().getName() + " (in " + item.getValue().getLocation() + ") ";
        }
        return returnVal;
    }
}