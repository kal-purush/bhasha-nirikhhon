package com.groupthree.mancala;

import com.groupthree.mancala.gameplay.Board;
import com.groupthree.mancala.gameplay.Store;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.input.MouseEvent;
import javafx.scene.shape.Circle;
import javafx.scene.shape.Rectangle;

public class GameController {
    @FXML
    Label row1hole1Value;
    @FXML
    Label row1hole2Value;
    @FXML
    Label row1hole3Value;
    @FXML
    Label row1hole4Value;
    @FXML
    Label row1hole5Value;
    @FXML
    Label row1hole6Value;
    @FXML
    Label row2hole1Value;
    @FXML
    Label row2hole2Value;
    @FXML
    Label row2hole3Value;
    @FXML
    Label row2hole4Value;
    @FXML
    Label row2hole5Value;
    @FXML
    Label row2hole6Value;

    @FXML
    Circle row1Hole1;
    @FXML
    Circle row1Hole2;
    @FXML
    Circle row1Hole3;
    @FXML
    Circle row1Hole4;
    @FXML
    Circle row1Hole5;
    @FXML
    Circle row1Hole6;
    @FXML
    Circle row2Hole1;
    @FXML
    Circle row2Hole2;
    @FXML
    Circle row2Hole3;
    @FXML
    Circle row2Hole4;
    @FXML
    Circle row2Hole5;
    @FXML
    Circle row2Hole6;

    @FXML
    Label playerOneUsername;

    @FXML
    Rectangle playerOneStore;
    @FXML
    Label playerOneStoreValue;

    @FXML
    Rectangle payerTwoStore;
    @FXML
    Label playerTwoStoreValue;

    @FXML
    Label playerTwoUsername;


    private final Board board;


    public GameController() {
        this.board = new Board();
    }

    public void moveMade(MouseEvent event) {
        Circle hole = (Circle) event.getSource();
        String id = hole.getId();
        switch (id) {
            case "row1Hole1", "row2Hole1" -> makeMove(1);
            case "row1Hole2", "row2Hole2" -> makeMove(2);
            case "row1Hole3", "row2Hole3" -> makeMove(3);
            case "row1Hole4", "row2Hole4" -> makeMove(4);
            case "row1Hole5", "row2Hole5" -> makeMove(5);
            case "row1Hole6", "row2Hole6" -> makeMove(6);
        }
        updateStones();
    }

    private void updateStones() {
        var row1 = board.getGameRows()[0];
        var row2 = board.getGameRows()[1];
        row1hole1Value.setText(String.valueOf(row1[0].checkStoneCount()));
        row1hole2Value.setText(String.valueOf(row1[1].checkStoneCount()));
        row1hole3Value.setText(String.valueOf(row1[2].checkStoneCount()));
        row1hole4Value.setText(String.valueOf(row1[3].checkStoneCount()));
        row1hole5Value.setText(String.valueOf(row1[4].checkStoneCount()));
        row1hole6Value.setText(String.valueOf(row1[5].checkStoneCount()));
        row2hole1Value.setText(String.valueOf(row2[0].checkStoneCount()));
        row2hole2Value.setText(String.valueOf(row2[1].checkStoneCount()));
        row2hole3Value.setText(String.valueOf(row2[2].checkStoneCount()));
        row2hole4Value.setText(String.valueOf(row2[3].checkStoneCount()));
        row2hole5Value.setText(String.valueOf(row2[4].checkStoneCount()));
        row2hole6Value.setText(String.valueOf(row2[5].checkStoneCount()));

        Store playerOneStore = board.getPlayer1Store();
        Store playerTwoStore = board.getPlayer2Store();

        playerOneStoreValue.setText(String.valueOf(playerOneStore.getNumberOfStonesInStore()));
        playerTwoStoreValue.setText(String.valueOf(playerTwoStore.getNumberOfStonesInStore()));


    }

    public void initializeNewGame() {
        updateStones();
    }


    private void makeMove(int holeNumber) {
        board.moveStones(holeNumber);
    }


}