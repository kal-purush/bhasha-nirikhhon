package com.bridgelabz;

import java.util.ArrayList;

public class Player {
    Card[] playerCards =new Card[9];
    static ArrayList<Player> playerList=new ArrayList<>();

    public static void addPlayer(Player player){
        playerList.add(player);
    }
}