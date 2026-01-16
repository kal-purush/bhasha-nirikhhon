package controllers;

import controllers.games.GameWithComputer;
import controllers.games.GameWithPlayer;
import controllers.reader.ConsoleReader;
import model.Player;
import view.ConsoleView;

public class Settings {

    private final ConsoleReader consoleReader = new ConsoleReader();
    private final ConsoleView consoleView = new ConsoleView();
    private WinnerController winnerController = new WinnerController();

    public void userSettings(){

        // Выбор варианта игры - с другим игроком или компьютером
        consoleView.consolePrint("\nPush 1 to play with friends, push 2 to play with computer!");

        final String youChoice = consoleReader.reader().trim();

        switch(youChoice){

            case "1":{
                GameWithPlayer gameWithPlayer = new GameWithPlayer(createUsers(false));
                gameWithPlayer.startGame();
                break;
            }
            case "2":{
                GameWithComputer gameWithComputer = new GameWithComputer(createUsers(true));
                gameWithComputer.startGame();
                break;
            }
            default:{
                consoleView.consolePrint("You input wrong value, please try again!");
                userSettings();
            }
        }
    }

    private Player[] createUsers(boolean withComputer) {

        String firstUser;
        String secondUser;

        if(!withComputer) {
            consoleView.consolePrint("\nInput first player name - figure 'X': ");
            firstUser = consoleReader.reader().trim();

            consoleView.consolePrint("\nInput second player name - figure 'O': ");
            secondUser = consoleReader.reader().trim();
        }
        else {
            consoleView.consolePrint("\nInput your name - figure 'X': ");
            firstUser = consoleReader.reader().trim();
            secondUser = "Bot";

        }

        Player[] players = new Player[]{
                new Player(firstUser),
                new Player(secondUser)
        };
        winnerController.setPlayers(players);
        return players;
    }
}