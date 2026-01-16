package com.character;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * A singleton class holding NPC information from NPC.json
 */
enum Cast {
    CAST;

    /**
     * properties
     */
    Map<String, Map<String, ArrayList<String>>> characters;

    /**
     * method to generate the map
     */
    Cast() {
        JSONParser parser = new JSONParser();
        characters = new LinkedTreeMap<>();
        try {
            JSONObject characterSet = (JSONObject) parser.parse(new FileReader("cfg/NPC.json"));
            characters = new Gson().fromJson(characterSet.toString(), LinkedTreeMap.class);
        } catch (FileNotFoundException | ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Map<String, Map<String, ArrayList<String>>> getCast() {
        return characters;
    }

}