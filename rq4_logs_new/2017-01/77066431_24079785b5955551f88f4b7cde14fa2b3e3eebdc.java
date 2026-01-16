package com.example.gorent.entidade;

import com.google.firebase.database.Exclude;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by orlandoamorim on 15/01/17.
 */

public class Client {

    public String name;
    public String id;
    public String adress;
    public String cnh;
    public String dOb;
    public String key;

    public Client(){
        // Default constructor required for calls to DataSnapshot.getValue(Carro.class);
    }

    public Client(String name, String id, String adress){
        this.name = name;
        this.id = id;
        this.adress = adress;
    }

    @Exclude
    public Map<String, Object> toMap(){
        HashMap<String, Object> result = new HashMap<>();
        result.put("name", name);
        result.put("id", id);
        result.put("adress", adress);

        return result;
    }
}