package Models;

import java.util.ArrayList;

public class User {

    private String username;
    private String password;
    private ArrayList<String> skills;

    public User(){
        skills = new ArrayList<String>();
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSkill(String skill) {
        this.skills.add(skill);
    }
}