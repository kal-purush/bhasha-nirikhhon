package pl.gacik.tictac;

import pl.gacik.tictac.boards.BoardDrawer;
import pl.gacik.tictac.boards.CrossIBoard;
import pl.gacik.tictac.boards.IBoard;
import pl.gacik.tictac.boards.LimitedCrossIBoard;

import java.util.Scanner;
import java.util.function.Consumer;
import java.util.function.Supplier;


public class GameRunner {

    //    static CrossIBoard board = new CrossIBoard();
    static CrossIBoard board = new LimitedCrossIBoard(9, 12);

    static GameSettings gameSettings = new GameSettings();
    static GameInitializer gameInitializer = new GameInitializer(gameSettings);

    static Consumer<String> mainConsumer = System.out::println;
    static Supplier<String> mainSupplier = () -> new Scanner(System.in).next();

    static BoardDrawer boardDrawer;

    static Tournament tournament;

    public static void main(String[] args) throws IBoard.FieldCheckException {

        gameInitializer.setLanguage(mainConsumer, mainSupplier);
        mainConsumer.accept(gameSettings.getMessagesProvider().greetings());
        gameInitializer.setPlayersAmount(mainConsumer, mainSupplier);
        for (int i = 0; i < gameSettings.getPlayersAmount(); i++) {
            gameInitializer.setPlayer(mainConsumer, mainSupplier);
        }
        gameInitializer.setTournamentAmount(mainConsumer, mainSupplier);
        Player[] players = gameSettings.getSignHolder().getAttachedPlayers().toArray(new Player[0]);
        int tournamentCounter = 0;
        while (tournamentCounter < gameSettings.getMaxRoundAmount() && ((gameSettings.getMaxRoundAmount() - tournamentCounter) * gameSettings.getWinPoints().points) > gameSettings.getPointsHolder().getHighestDifference().points) {
            mainConsumer.accept(gameSettings.getMessagesProvider().round() + " " + (tournamentCounter + 1));
            TurnHolder turnHolder = new TurnHolder(gameSettings.getSignHolder().getAttachedPlayers().toArray(new Player[0]));
            for (int i = 0; i < tournamentCounter; i++) {
                turnHolder.getNextPlayer();
            }
            gameInitializer.setBoard(mainConsumer, mainSupplier);
            boardDrawer = new BoardDrawer((CrossIBoard) gameSettings.getBoard());
            boardDrawer.draw();
            gameInitializer.setWinCheckers(mainConsumer, mainSupplier);
            tournament = new Tournament(gameSettings);
            tournament.setTurnHolder(turnHolder);
            tournament.startGame(mainConsumer, mainSupplier);
            tournamentCounter++;
            new PointsSumUpDrawer(gameSettings.getPointsHolder(), gameSettings.getMessagesProvider()).draw(mainConsumer);
        }
        mainConsumer.accept(gameSettings.getMessagesProvider().masterOfGame() + " " + gameSettings.getPointsHolder().getWinner().get());
    }
}