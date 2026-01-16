package com.kakaotalk.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Room {
    private String name;
    private User owner;
    private Map<String, User> users = new HashMap<>();

    public Room(String name, User owner) {
        this.name = name;
        this.owner = owner;
        addUser(owner);
    }

    public List<String> getUserList() {
        return new ArrayList<>(users.keySet());
    }

    public void addUser(User user) {
        users.put(user.getUsername(), user);
    }

    public void removeUser(User user) {
        users.remove(user.getUsername());
    }

    public boolean containsUser(User user) {
        return users.containsKey(user.getUsername());
    }

    public boolean isOwner(User user) {
        return owner.equals(user);
    }

    @Override
    public String toString() {
        return name;
    }
}
