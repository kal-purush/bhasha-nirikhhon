package server;

import server.Tile.Bag;

public class Game {

    final static int MAX_PLAYERS = 4;

    String name;
    Board board;
    Bag bag;
    Player[] players;
    Player currentPlayer;
    int rounds;
    int currentRound;

    public Game(String name, String[] fileNames, Player host, int rounds) {
        this.name = name;
        this.board = new Board(fileNames);
        this.bag = new Bag();
        this.players = new Player[]{host};
        this.rounds = rounds;
    }

    public void orderPlayers() {
        throw new UnsupportedOperationException();
    }

    public void setup() {
        this.orderPlayers();
        currentRound = 1;
        currentPlayer = players[0];
    }

    public boolean isOver() {
        return currentRound == rounds;
    }

    public void addPlayer(Player player) {
        // Note to players limit
        throw new UnsupportedOperationException();
    }

    public void playTurn(Player player, Word word) {
        if (player != currentPlayer) {
            player.sendToPlayer.accept("Player " + player.name + " is not the current player");
            return;
        }
        playCurrentTurn(word);
    }

    private void playCurrentTurn(Word word) {
        if (!currentPlayer.hasTiles(word.getTiles())) {
            currentPlayer.notifyMissingTilesForWord(word);
            return;
        }
        int wordScore = board.tryPlaceWord(word);
        if (wordScore == 0) {
            currentPlayer.notifyIllegalWord(word);
            return;
        }
        currentPlayer.addScore(wordScore);
        int wordTilesCount = word.getTiles().length;
        currentPlayer.replaceTiles(word.getTiles(), bag.getRandomTiles(wordTilesCount));
        this.advanceCurrentPlayer();
        currentRound += 1;
        this.sendGameToPlayers();
    }

    private void sendGameToPlayers() {
        for (Player player : players) {
            player.sendBoard(board);
            player.sendBag(bag);
            player.sendPlayers(players);
            player.sendCurrentPlayer(currentPlayer.name);
            player.sendCurrentRound(currentRound);
        }
    }

    private void advanceCurrentPlayer() {
        throw new UnsupportedOperationException();
    }
}