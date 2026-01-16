package de.romjaki.discordrcon;

import net.dv8tion.jda.core.entities.User;
import org.json.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class UserMapping {
    private static final File mappingFile = new File("users.json");
    private static Map<String, String> discordIdToMinecraftUserName = new HashMap<>();

    private UserMapping() {

    }

    @PublicAPI
    public static String getMinecraftUser(String discordId) {
        return discordIdToMinecraftUserName.get(discordId);
    }

    @PublicAPI
    public static String getMinecraftUser(User user) {
        return getMinecraftUser(user.getId());
    }

    @PublicAPI
    public static boolean hasMinecraftUserAsociated(User user) {
        return hasMinecraftUserAsociated(user.getId());
    }

    @PublicAPI
    public static boolean hasMinecraftUserAsociated(String id) {
        return discordIdToMinecraftUserName.containsKey(id);
    }

    @PublicAPI
    public static String replaceMinecraftUserName(User user, String newUsername) {
        return replaceMinecraftUserName(user.getId(), newUsername);
    }

    @PublicAPI
    public static String replaceMinecraftUserName(String id, String newUsername) {
        return discordIdToMinecraftUserName.put(id, newUsername);
    }

    public static void load() {
        try (Scanner s = new Scanner(mappingFile).useDelimiter("\\A")) {
            loads(s.next());
        } catch (FileNotFoundException e) {
            try {
                if (mappingFile.createNewFile()) {
                    System.out.println("Found no users.json file. Creating default one.");
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(mappingFile))) {
                        bos.write("{}".getBytes());
                    }
                }
            } catch (IOException e1) {
                System.err.println("Failed to create user mapping file. Check that you have write permissions for the current directory.");
                System.exit(1);
            }
        }
    }

    private static void loads(String json) {
        JSONObject object = new JSONObject(json);
        for (String id : object.keySet()) {
            discordIdToMinecraftUserName.put(id, object.getString(id));
        }
    }

    @PublicAPI
    public static boolean isInUse(String account) {
        return discordIdToMinecraftUserName.containsValue(account);
    }
}