package com.groupthree.mancala.gameplay;

import java.util.ArrayList;
import java.util.List;

public class Board {
    private Hole[][] gameBoard;
    private Store player1Store;
    private Store player2Store;
    private boolean playerOneTurn;
    List<Stone> stonesInHand = new ArrayList<>();
    //TODO if stones in hand finishes in user store

    public Board() {
        playerOneTurn = true;
        gameBoard = new Hole[2][6];
        for (Hole[] row : gameBoard) {
            for (int i = 0; i < row.length; i++) {
                Stone stone1 = new Stone();
                Stone stone2 = new Stone();
                Stone stone3 = new Stone();
                Stone stone4 = new Stone();
                Hole hole = new Hole(stone1, stone2, stone3, stone4);
                row[i] = hole;

            }
        }
        player1Store = new Store();
        player2Store = new Store();
    }

    public void moveStones(int from) {
        Hole[] playerOneRow = gameBoard[0];
        Hole[] playerTwoRow = gameBoard[1];
        if (playerOneTurn) {
            if (from > 0 && from < 7) {
                // get index of the hole we want to remove stones from
                int currentIndex = from - 1;
                // retrieve the hole from the player row
                Hole hole = playerOneRow[currentIndex];
                // if there are no stones in hand
                if (stonesInHand.isEmpty()) {
                    // remove all stones in the current hole and place them in hand
                    stonesInHand = hole.removeStones();
                    // increment start index for loop since we have already removed stones from the current hole
                    currentIndex = currentIndex - 1;
                }
                // since we are in the first row to move counter clock wise, we need to
                // loop back from the current hole index to 0
                for (int i = currentIndex; i >= 0; i--) {
                    // get next hole to remove stone from
                    Hole currentHole = playerOneRow[i];
                    // if we have more than one stone in hand
                    if (stonesInHand.size() > 1) {
                        // remove stone from our hand and add to the current hole
                        currentHole.addStones(stonesInHand.remove(0));
                    } else if (stonesInHand.size() == 1) {
                        // if we have only one stone in hand
                        // check to see if the current hole is empty
                        if (currentHole.isEmpty()) {
                            // if empty add the stone in hand to the hole and end turn
                            currentHole.addStones(stonesInHand.remove(0));
                            playerOneTurn = false;
                            return;
                        } else {
                            // if the current hole is not empty add stone to current hole
                            currentHole.addStones(stonesInHand.remove(0));
                            // remove all stones from current hole and add to hand
                            stonesInHand = currentHole.removeStones();
                        }
                    }
                }
                // if after going through the entire first row, we still have stones in hand
                if (stonesInHand.size() > 0) {
                    // add stone to player 1 store
                    Stone temp = stonesInHand.remove(0);
                    player1Store.addStoneToStore(temp);
                }
                // if after adding stone to store we still have stones in hand
                if (stonesInHand.size() > 0) {
                    // distribute stones across the second row
                    for (int i = 0; i < playerTwoRow.length; i++) {
                        Hole nextHole = playerTwoRow[i];
                        if (stonesInHand.size() > 1) {
                            nextHole.addStones(stonesInHand.remove(0));
                        } else if (stonesInHand.size() == 1) {
                            if (nextHole.isEmpty()) {
                                nextHole.addStones(stonesInHand.remove(0));
                                playerOneTurn = false;
                                return;
                            } else {
                                nextHole.addStones(stonesInHand.remove(0));
                                stonesInHand = nextHole.removeStones();
                            }
                        }
                    }
                    // if after distributing across second row we still have stones in hand,
                    // start process again from
                    if (stonesInHand.size() > 0) {
                        moveStones(6);
                    }
                }
                playerOneTurn = false;
            } else {
                throw new IllegalArgumentException("Player One can only choose from 1 to 6");
            }
        }
    }

    public boolean isPlayerOneTurn(){
        return playerOneTurn;
    }

    private void makeMovesInPlayerOneRow(int currentHoleIndex, Hole[] playerOneRow, List<Stone> currentSetOfStones) {
        for (int i = currentHoleIndex - 1; i >= 0; i--) {
            Hole nextHole = playerOneRow[i];
            if (currentSetOfStones.size() > 1) {
                nextHole.addStones(currentSetOfStones.remove(0));
            } else if (currentSetOfStones.size() == 1) {
                if (nextHole.isEmpty()) {
                    nextHole.addStones(currentSetOfStones.remove(0));
                    return;
                } else {
                    nextHole.addStones(currentSetOfStones.remove(0));
                    currentSetOfStones = nextHole.removeStones();
                }
            }
        }
    }

    private void makeMoveInNextRow() {
    }


    public void displayBoard() {
        System.out.println("player1Store {" + player1Store.getNumberOfStonesInStore() + "}");
        for (Hole[] row : gameBoard) {
            for (Hole hole : row) {
                System.out.print(hole + " ");
            }
            System.out.println();
        }
        System.out.println("player2Store {" + player2Store.getNumberOfStonesInStore() + "}");
    }

    public boolean checkIfGameOver() {
        return false;
    }

    public int getWinner() {
        return -1;
    }


}