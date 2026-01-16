package com.engine;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Instruction {
    Map<String, Map<String, List<String>>> instruct;

    /**
     * method to generate the map
     */
    Instruction() {
        JSONParser parser = new JSONParser();
        instruct = new LinkedTreeMap<>();
        try {
            JSONObject characterSet = (JSONObject) parser.parse(new FileReader("cfg/extendedFile.json"));
            instruct = new Gson().fromJson(characterSet.toString(), LinkedTreeMap.class);
        } catch (FileNotFoundException | ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    Map<String, Map<String, List<String>>> getInstruction() {
        return instruct;
    }
}