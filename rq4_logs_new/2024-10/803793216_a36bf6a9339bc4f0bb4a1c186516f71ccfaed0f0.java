package org.springframework.samples.gitvision.file.model;

import org.springframework.samples.gitvision.change.model.Change;

import java.util.HashMap;
import java.util.Map;

public class ChangesByUser {
    
    private Map<String, Change> changesByUser;

    public ChangesByUser(){
        this.changesByUser = new HashMap<>();
    }

    public void add(String user, Change change){
        if(changesByUser.containsKey(user)){
            changesByUser.get(user).merge(change);
        } else {
            changesByUser.put(user, change);
        }
    }

    @Override
    public String toString() {
        return this.changesByUser.toString();
    }

}