package javafxgui.app;

import javafxgui.javafxcomponents.GUIView;
import jttt.core.board.Board;
import jttt.core.game.GameMaker;

public class GUIAppSpy extends GUIApp {
    private boolean hasGameStartBeenCalled = false;
    private boolean hasBoardBeenDisplayedToUI = false;

    public GUIAppSpy(GameMaker gameMaker, GUIView guiView) {
        super(gameMaker, guiView);
    }

    @Override
    public void displayGameLayout(Board board) {
        hasBoardBeenDisplayedToUI = true;
    }

    @Override
    public void startGame() {
        hasGameStartBeenCalled = true;
    }

    public boolean hasGameStartBeenCalled() {
        return hasGameStartBeenCalled;
    }

    public boolean hasBoardBeenDisplayedToUI() {
        return hasBoardBeenDisplayedToUI;
    }
}