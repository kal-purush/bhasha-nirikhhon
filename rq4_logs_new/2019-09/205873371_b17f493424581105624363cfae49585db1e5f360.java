package com.kodilla.game.engine;

import com.kodilla.game.ElementalClash;
import com.kodilla.game.creatures.*;
import javafx.scene.layout.FlowPane;

import java.util.Random;

public class AIdecidionsMaker {

    private static Creature[] availableCreatures = {new WaterWave(), new Merfolk(), new WaterElemental(), new SeaSerpent()};
    private static boolean creatureCreated = false;


    private static Creature creaturePicker(Player player){
        Random random = new Random();
        int i =0;
        do {
        i = random.nextInt(4);
        }
        while(player.getCurrentMana() <= availableCreatures[i].getManaCost());
        return availableCreatures[i];
    }

    public static void makeDecision(Player player, OccupationChecker checker, FlowPane pane){
        Random random = new Random();
        int i = random.nextInt(2);
        switch(i){
            case 0: player.setCurrentMana(player.getCurrentMana() + 2);
            case 1: {
                for (int k = 0; k < 6; k++){
                    while(!checker.isOccupied(k)) {
                        int j = random.nextInt(6);
                        while (!checker.isOccupied(j)) {
                            ElementalClash.createCreature(player, j, AIdecidionsMaker.creaturePicker(player), pane, checker);
                            creatureCreated = true;
                        }
                    }
                }
                if(!creatureCreated) {
                    player.setCurrentMana(player.getCurrentMana() + 2);
                }
                creatureCreated = false;
            }
        }
    }
}