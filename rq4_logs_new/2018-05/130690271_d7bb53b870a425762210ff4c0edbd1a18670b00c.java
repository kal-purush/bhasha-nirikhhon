package pl.gacik.tictac;

import pl.gacik.coordinates.ICoordinates2D;
import pl.gacik.coordinates.SimpleICoordinates2D;

import java.util.function.Consumer;
import java.util.function.Supplier;

public class Tournament {

    private GameSettings gameSettings;


    public Tournament(GameSettings gameSettings) {
        if(gameSettings == null) {
            throw new IllegalArgumentException("gameSettings cannot be null");
        }
        this.gameSettings = gameSettings;
    }

    public TurnHolder getTurnHolder() {
        return turnHolder;
    }

    public void setTurnHolder(TurnHolder turnHolder) {
        if(turnHolder == null) {
            throw new IllegalArgumentException("turnHolder cannot be null");
        }
        if(turnHolder.getPlayersAmount() < 2) {
            throw new IllegalArgumentException("Not enough players for game");
        }
        this.turnHolder = turnHolder;
    }

    private TurnHolder turnHolder;

    public void startGame(Consumer<String> out, Supplier<String> in) throws IBoard.FieldCheckException {
        boolean isWinner = false;
        while(gameSettings.getBoard().hasAvailableField()) {
                new BoardDrawer((CrossIBoard) gameSettings.getBoard()).draw();
                Player player = turnHolder.getNextPlayer();
                out.accept(gameSettings.getMessagesProvider().introductionToTurn() + " " + player.getName());
                out.accept(gameSettings.getMessagesProvider().askForCoordinateX());
                int x = Integer.parseInt(in.get());
                out.accept(gameSettings.getMessagesProvider().askForCoordinateY());
                int y = Integer.parseInt(in.get());
                ICoordinates2D coordinates2D = new SimpleICoordinates2D(x,y);
                gameSettings.getBoard().addPair(coordinates2D, gameSettings.getSignHolder().getBookedSign(player).get());
                if (victoryAchieved(coordinates2D)) {
                    isWinner = true;
                    new BoardDrawer((CrossIBoard) gameSettings.getBoard()).draw();
                    out.accept(gameSettings.getMessagesProvider().winnerAnnouncing() + " " + player.getName());
                    break;
                }
        }
        if(!isWinner) {
            new BoardDrawer((CrossIBoard) gameSettings.getBoard()).draw();
            out.accept(gameSettings.getMessagesProvider().draw());
        }
    }

    private boolean victoryAchieved(ICoordinates2D lastMove) {
        return (gameSettings.getWinCheckers().stream().filter(w -> w.victoryAchieved(lastMove)).count() != 0);
    }


}