package org.launchcode.cheesemvc.models;

import java.util.ArrayList;

public class UserData {

    static ArrayList<User> users = new ArrayList<>();

    public static ArrayList<User> getAll(){
        return users;
    }

    public static void add(User newUser) {
        users.add(newUser);
    }

    public static void remove(User oldUser) {
        users.remove(oldUser);
    }

    public static User getById(int id) {
        User theUser = null;

        for (User user : users) {
            if (user.getUserId() == id) {
                theUser = user;
            }
        } return theUser;
    }



}