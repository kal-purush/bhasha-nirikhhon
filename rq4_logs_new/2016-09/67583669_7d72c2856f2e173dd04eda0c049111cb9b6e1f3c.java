package com.Example.JunitRestAssuredDemo;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

/**
 * Created by vishal on 9/7/16.
 */
public class JsonParser {
    public static Map<String, Object> jsonToMap(JSONObject json) throws JSONException {
        Map<String, Object> retMap = new HashMap<String, Object>();

        if (json != JSONObject.NULL) {
            retMap = toMap(json);
        }
        return retMap;
    }

    public static Map<String, Object> toMap(JSONObject object) throws JSONException {
        Map<String, Object> map = new HashMap<String, Object>();

        Iterator<String> keysItr = object.keys();
        while (keysItr.hasNext()) {
            String key = keysItr.next();
            Object value = object.get(key);

            if (value instanceof JSONArray) {
                value = toList((JSONArray) value);
            } else if (value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            map.put(key, value);
        }
        return map;
    }

    public static List<Object> toList(JSONArray array) throws JSONException {
        List<Object> list = new ArrayList<Object>();
        for (int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                value = toList((JSONArray) value);
            } else if (value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            list.add(value);
        }
        return list;
    }

    public String readJsonFromUrl(String json_url) throws Exception {
        URL website = new URL(json_url);
        StringBuilder response = new StringBuilder();
        URLConnection connection = website.openConnection();
          try {

              BufferedReader in = new BufferedReader(
                      new InputStreamReader(
                              connection.getInputStream()));
              String inputLine;
              while ((inputLine = in.readLine()) != null)
                  response.append(inputLine);
              in.close();
          }catch (ConnectException ex){
//              ex.printStackTrace();
              throw new ConnectException();
          }
            return response.toString();
    }

    public List stringJsonArrayToJsonArray(String json) {
        try {
            JSONArray jsonArray = new JSONArray(json);

            return toList(jsonArray);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Map stringJsonToJsonArray(String json) {
        try {
            JSONObject jsonObject = new JSONObject(json);
            return jsonToMap(jsonObject);
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return null;
    }

}