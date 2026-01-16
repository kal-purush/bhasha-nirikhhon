package io.github.jeremyhu.fishapi;

import com.sun.net.httpserver.HttpExchange;
import org.bukkit.Bukkit;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.entity.Player;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class APIHandler {
    private HttpExchange e;
    private HashMap<String,String> query;
    private FileConfiguration players;
    private Integer type;
    private String response;

    public APIHandler(HttpExchange e,FileConfiguration players,int type){
        this.players = players;
        this.e = e;
        this.type = type;
    }

    public void response(){
        switch(type){
            case 1:
                JSONObject obj = new JSONObject();
                List<String> players = new ArrayList<String>();
                for(Player p : Bukkit.getOnlinePlayers()){
                    players.add(p.getName());
                }
                obj.put("onlines",players);
                response =  obj.toString();
        }
        try {
            e.sendResponseHeaders(200,response.getBytes().length);
            OutputStream body = e.getResponseBody();
            body.write(response.getBytes());
            body.flush();
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
        e.close();
    }
}