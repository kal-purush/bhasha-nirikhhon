package javafxgui.javafxcomponents;

import javafx.scene.control.Button;
import javafx.scene.layout.GridPane;
import javafxgui.event.ClickEventHandler;
import jttt.Core.Board.Board;

public class JavaFxBoardComponent extends GridPane {
    private final String GAME_BOARD_ID = "gameBoard";
    private final ClickEventHandler eventHandler;
    private int POSITION_OFFSET = 1;

    public JavaFxBoardComponent(Board board, ClickEventHandler eventHandler) {
        setId(GAME_BOARD_ID);
        this.eventHandler = eventHandler;
        addBoardCells(board);
    }

    private void addBoardCells(Board board) {
        int position = 0;
        for (int row = 0; row < board.getDimension(); row++) {
            for (int col = 0; col < board.getDimension(); col++) {
                Button cell = getButton(board, position);
                registerButtonWithEventHandler(cell);
                add(cell, col, row);
                position++;
            }
        }
    }

    private Button getButton(Board board, int position) {
        Button button = new Button(board.findMarkAtIndex(position).markOrPositionForDisplay(position));
        button.setId(generateButtonId(position));
        return button;
    }

    private String generateButtonId(int position) {
        return String.valueOf(position + POSITION_OFFSET);
    }

    private void registerButtonWithEventHandler(Button button) {
        JavaFXButton jfxButton = new JavaFXButton(button);
        jfxButton.setOnAction(eventHandler);
    }
}