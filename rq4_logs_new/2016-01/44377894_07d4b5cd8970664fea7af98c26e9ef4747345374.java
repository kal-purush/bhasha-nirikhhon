package javafxgui;

import jttt.UI.UserInterface;
import jttt.core.game.GameMaker;
import jttt.core.board.Board;
import jttt.core.game.Game;
import jttt.core.players.Player;
import jttt.core.players.PlayerFactory;

import java.util.List;

public class GUIGameMaker implements GameMaker {
    private final PlayerFactory playerFactory;

    public GUIGameMaker(PlayerFactory playerFactory) {
        this.playerFactory = playerFactory;
    }

    @Override
    public Game initializeGame(int boardDimension, int gameTypeOption, UserInterface ui) {
        List<Player> players = playerFactory.findPlayersFor(gameTypeOption);
        return new Game(new Board(boardDimension), players.get(0), players.get(1), new GUIDisplayer());
    }
}