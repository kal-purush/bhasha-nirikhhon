package de.romjaki.discordrcon;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Config {
    public static JSONObject config;
    public static JSONObject rcon;
    public static int port;
    public static String password;
    public static String host;
    public static JSONObject discord;
    public static String token;
    public static List<String> admins;
    public static boolean selfInvite;

    static {
        try (Scanner s = new Scanner(new File("config.json")).useDelimiter("\\A")) {
            config = new JSONObject(s.next());
        } catch (FileNotFoundException e) {
            System.out.println("config.json not found!");
        }
        rcon = config.getJSONObject("rcon");
        port = rcon.getInt("port");
        host = rcon.getString("server");
        password = rcon.getString("password");
        discord = config.getJSONObject("discord");
        token = discord.getString("token");
        JSONArray temp = discord.getJSONArray("admins");
        admins = new ArrayList<>();

        temp.forEach(o -> admins.add(o.toString()));
        selfInvite = config.getBoolean("selfinvite");
    }
}