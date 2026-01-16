package com.groupthree.mancala.gameplay;

import java.util.HashMap;
import java.util.Map;

public class ArcadeBoard extends Board {
    private Map<PowerUp, Boolean> playerOnePowerUps;
    private Map<PowerUp, Boolean> playerTwoPowerUps;


    public ArcadeBoard() {
        super();
        playerOnePowerUps = new HashMap<>();
        playerOnePowerUps.put(PowerUp.DOUBLE_POINTS, false);
        playerOnePowerUps.put(PowerUp.CONTINUE_TURN, false);
        playerTwoPowerUps = new HashMap<>();
        playerTwoPowerUps.put(PowerUp.DOUBLE_POINTS, false);
        playerTwoPowerUps.put(PowerUp.CONTINUE_TURN, false);
    }

    public void boostPointsInStore(int playerNumber, int points) {
        var store = playerNumber == 1 ? getPlayer1Store() : getPlayer2Store();
        for (int i = 0; i < points; i++) {
            store.addStoneToStore(new Stone());
        }
    }

    public void usePowerUp(PowerUp powerUp, int userNumber) {
        if (userNumber == 1) {
            playerOnePowerUps.replace(powerUp, false, true);
        } else if (userNumber == 2) {
            playerTwoPowerUps.replace(powerUp, false, true);
        }
    }

    public boolean getPowerUpStatus(PowerUp powerUp, int userNumber) {
        if (userNumber == 1) {
            return playerOnePowerUps.get(powerUp);
        } else if (userNumber == 2) {
            return playerTwoPowerUps.get(powerUp);
        }
        return false;
    }
}