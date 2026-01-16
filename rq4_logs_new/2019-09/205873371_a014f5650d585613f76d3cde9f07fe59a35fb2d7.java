package com.kodilla.game.engine;

import com.kodilla.game.ElementalClash;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.FlowPane;

public class NewGameStarter {

    public static void startNewGame(Player player, Player computer, Label myManaLbl, Label computerManaLbl,
                                    OccupationChecker playerChecker, OccupationChecker AIChecker, FlowPane playerPane, FlowPane AIPane,
                                    FlowPane playerStatsPane, FlowPane AIStatsPane, Button saveMana, FlowPane buyButtons, Label statusLabel,
                                    FlowPane putButtons){
        player.setCurrentLife(30);
        player.setCurrentMana(5);
        player.lifeLbl.setText("Życie: " + player.getCurrentLife());
        myManaLbl.setText("Mana: " + player.getCurrentMana());
        for(int i = 0; i < 6; i++){
            if(playerChecker.isOccupied(i)){
                ElementalClash.killCreature(player, i, player.checkCreature(i), playerPane, playerStatsPane, playerChecker);
            }
        }

        computer.setCurrentLife(30);
        computer.setCurrentMana(5);
        computer.lifeLbl.setText("Życie: " + computer.getCurrentLife());
        computerManaLbl.setText("Mana: " + computer.getCurrentMana());
        for(int i = 0; i < 6; i++){
            if(AIChecker.isOccupied(i)){
                ElementalClash.killCreature(computer, i, computer.checkCreature(i), AIPane, AIStatsPane, AIChecker);
            }
        }

        for(int i = 0; i < 4; i ++)
        {
            buyButtons.getChildren().get(i).setVisible(true);
        }
        for(int i = 0; i < 6; i ++)
        {
            putButtons.getChildren().get(i).setVisible(false);
        }
        saveMana.setVisible(true);
        statusLabel.setText("Twoja tura");
    }

}