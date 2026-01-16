package com.softdesign.school.data.storage.models;

import com.activeandroid.Model;
import com.activeandroid.annotation.Column;
import com.activeandroid.annotation.Table;
import com.activeandroid.query.Select;

import java.util.ArrayList;
import java.util.List;

@Table(name = "team")
public class Team  extends Model{
    @Column(name = "name")
    private String name;
    public Team(){
        super();
    }

    public Team(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
    public static List<Team> getAll (){
        return new Select().from(Team.class).execute();
    }
    public static List<String> getAllNames (){
        List<Team> teams = getAll();
        List<String> names = new ArrayList<String>();
        for (Team t: teams){
            names.add(t.getName());
        }
        return names;
    }
    public static Team getByName(String name){
        return new Select().from(Team.class).where("name=?",name).executeSingle();
    }
}