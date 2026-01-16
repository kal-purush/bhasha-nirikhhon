package AI;

import Game.Board;

public abstract class AI {

    // Store the board for the AI to access
    protected Board board;

    // Store which player the AI is so it knows its turn
    protected int player;

    /**
     * Constructor that only gives the AI board data and no player data (used for Human vs AI games)
     */
    protected AI(Board board) {
        this.board = board;
    }

    /**
     * Constructor that gives the AI both board and player data
     */
    protected AI(Board board, int player) {
        this.board = board;
        this.player = player;
    }

    /**
     * @return an integer representing whether this AI is player 1 or 2
     */
    public int getPlayer() {
        return player;
    } 

    /**
     * @return the Board
     */
    public Board getBoard() {
        return board;
    }

    /**
     * Dummy implementation that subclasses override to support a more generic implementation
     * @return an arbitrary integer
     */
    public int getMove() {
        int temp = 0;
        return temp + 1;
    }

}