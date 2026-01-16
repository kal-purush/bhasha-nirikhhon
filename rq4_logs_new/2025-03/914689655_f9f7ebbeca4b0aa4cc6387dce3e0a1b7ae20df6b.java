package ui;

import chess.ChessGame;

import java.util.Arrays;
import java.util.List;

import static ui.EscapeSequences.*;

public class ChessSquare {
    private int rowsPrinted = 0;

    public String chessTopOrBottom(String backgroundColor){
        return backgroundColor + EMPTY + " " + EMPTY + RESET_BG_COLOR;
    }

    public String rowTopOrBottom(Boolean whiteFirst){
        String response = "";
        List<String> firstColor = Arrays.asList(SET_BG_COLOR_WHITE, SET_BG_COLOR_BLACK);
        if(!whiteFirst){
            firstColor = firstColor.reversed();
        }
        for(int i = 0; i < 4; i++){
            response += chessTopOrBottom(firstColor.get(0));
            response += chessTopOrBottom(firstColor.get(1));
        }

        return SET_BG_COLOR_LIGHT_GREY + "   " + RESET_BG_COLOR + response + SET_BG_COLOR_LIGHT_GREY + "   " + RESET_BG_COLOR;
    }

    public String chessMiddle(String pieceType, String squareColor){

            return squareColor + EMPTY + pieceType + EMPTY + RESET_BG_COLOR;
    }

    public String rowChessMiddle(List<String> rowPieces, ChessGame.TeamColor playerColor, Boolean whiteFirst){
        // include null for empty squares PLEASE
        String sideLabels = sideLabelMaker(playerColor);
        String squareColor = null;
        String altSquareColor = null;
        if(whiteFirst){
            squareColor = SET_BG_COLOR_WHITE;
            altSquareColor = SET_BG_COLOR_BLACK;
        }
        else{
            squareColor = SET_BG_COLOR_BLACK;
            altSquareColor= SET_BG_COLOR_WHITE;
        }

        String response = "";
        for(int i = 0; i < rowPieces.size(); i += 2){
            response += chessMiddle(rowPieces.get(i), squareColor);
            response += chessMiddle(rowPieces.get(i + 1), altSquareColor);
        }

        return sideLabels+ response + sideLabels;
    }

    private String sideLabelMaker(ChessGame.TeamColor playerColor){
        List<String> boardNumbers = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8");
        if(playerColor != ChessGame.TeamColor.BLACK){
            boardNumbers = boardNumbers.reversed();
        }
        String response = SET_BG_COLOR_LIGHT_GREY + " "+ SET_TEXT_COLOR_BLACK + boardNumbers.get(rowsPrinted) + " " +  RESET_BG_COLOR;
        rowsPrinted += 1;
        return response;
    }
}