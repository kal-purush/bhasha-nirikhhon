package com.octo.ioni.miudotko.Models;

/**
 * Created by ioni on 9/25/17.
 */

public class MatchPlayer {
    long account_id;
    int player_slot;
    int hero_id;

    public MatchPlayer(){
        initValues();
    }

    public MatchPlayer(long id, int player_slot, int hero_id){
        initValues();
        this.account_id = id;
        this.player_slot = player_slot;
        this.hero_id = hero_id;
    }

    private void initValues(){
        this.account_id = -1;
        this.player_slot = -1;
        this.hero_id = -1;
    }

    public long getAccount_id() {
        return account_id;
    }

    public long getAccount_id64() {
        return account_id ;
    }

    public int getHero_id() {
        return hero_id;
    }

    public int getPlayer_slot() {
        return player_slot;
    }
}