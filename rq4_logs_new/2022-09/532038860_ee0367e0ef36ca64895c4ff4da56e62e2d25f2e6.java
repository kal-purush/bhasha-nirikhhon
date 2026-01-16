package com.bridgelabz;

import java.util.ArrayList;

public class Player {
	
	Cards[] playerCards = new Cards[9];
    static ArrayList<Player> playerList = new ArrayList<>();
    public static void addPlayer(Player player){
        playerList.add(player);
    }

}